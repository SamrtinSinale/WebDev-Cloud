/**
 * 处理WebDAV GET请求
 * 用于获取文件内容
 *
 * Range/条件请求由 StorageStreaming 统一处理
 */
import { MountManager } from "../../storage/managers/MountManager.js";
import { getEncryptionSecret } from "../../utils/environmentUtils.js";
import { FileSystem } from "../../storage/fs/FileSystem.js";
import { createWebDAVErrorResponse, withWebDAVErrorHandling } from "../utils/errorUtils.js";
import { addWebDAVHeaders, getStandardWebDAVHeaders } from "../utils/headerUtils.js";
import { getEffectiveMimeType } from "../../utils/fileUtils.js";
import { LinkService } from "../../storage/link/LinkService.js";
import { StorageStreaming, STREAMING_CHANNELS } from "../../storage/streaming/index.js";
import { CAPABILITIES } from "../../storage/interfaces/capabilities/index.js";

// Windows MiniRedir 302自动降级：
// - 对同一路径，第一次请求按挂载策略走 302
// - 如果客户端再次通过 WebDAV GET 同一路径，则视为 302 可能不可靠，后续该路径强制走本地代理
const miniRedirTried302 = new Set();

/**
 * 从驱动返回结果中提取 URL
 * 当前 WebDAV 场景下，generateDownloadUrl 约定返回 { url, type, ... }
 * @param {*} result - 驱动返回的结果
 * @returns {string|null} 提取的 URL 或 null
 */
function extractUrlFromResult(result) {
  if (!result) return null;
  if (typeof result === "object") {
    return result.url || null;
  }
  return null;
}

/**
 * 通过 StorageStreaming 层下载文件（native_proxy）
 *
 *
 * @param {MountManager} mountManager - 挂载管理器
 * @param {string} path - 文件路径
 * @param {Object} c - Hono 上下文
 * @param {string} userId - 用户ID
 * @param {string} userType - 用户类型
 * @returns {Promise<Response>} WebDAV 响应
 */
async function downloadViaStreaming(mountManager, path, c, userId, userType) {
  console.log(`WebDAV GET - 使用 StorageStreaming 本地代理模式: ${path}`);

  const encryptionSecret = getEncryptionSecret(c);
  const streaming = new StorageStreaming({
    mountManager,
    storageFactory: null, // FS 路径模式不需要 storageFactory
    encryptionSecret,
  });

  // 获取 Range 头
  const rangeHeader = c.req.header("Range") || null;

  // 通过 StorageStreaming 创建响应
  const response = await streaming.createResponse({
    path,
    channel: STREAMING_CHANNELS.WEBDAV,
    rangeHeader,
    request: c.req.raw,
    userIdOrInfo: userId,
    userType,
    db: c.env.DB,
  });

  return addWebDAVHeaders(response);
}


/**
 * 处理GET请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} userId - 用户ID
 * @param {string} userType - 用户类型 (admin 或 apiKey)
 * @param {D1Database} db - D1数据库实例
 */
export async function handleGet(c, path, userId, userType, db) {
    const isHead = c.req.method === "HEAD";
    return withWebDAVErrorHandling("GET", async () => {
    const userAgent = c.req.header("User-Agent") || "";
    const isWindowsMiniRedirector =
      userAgent.includes("Microsoft-WebDAV") || userAgent.includes("WebDAV-MiniRedir");

    // 创建FileSystem实例
    const repositoryFactory = c.get("repos");
    const mountManager = new MountManager(db, getEncryptionSecret(c), repositoryFactory, { env: c.env });
    const fileSystem = new FileSystem(mountManager);

    // 获取文件名并统一从文件名推断MIME类型
    const fileName = path.split("/").pop();
    const contentType = getEffectiveMimeType(null, fileName);
    console.log(`WebDAV GET - 从文件名[${fileName}]推断MIME类型: ${contentType}`);

    // 处理条件请求头
    const ifNoneMatch = c.req.header("If-None-Match");
    const ifModifiedSince = c.req.header("If-Modified-Since");
    const ifMatch = c.req.header("If-Match");
    const ifUnmodifiedSince = c.req.header("If-Unmodified-Since");

    // 首先获取文件信息以检查条件请求
    let fileInfo;
    try {
      fileInfo = await fileSystem.getFileInfo(path, userId, userType);
    } catch (error) {
      if (error.status === 404) {
        return createWebDAVErrorResponse("文件不存在", 404);
      }
      throw error;
    }

    // RFC 7231 §4.3.1: GET/HEAD 作用于集合（目录）时应返回 405 并带 Allow 头，
    // 而不是让请求继续下沉到存储驱动（Telegram 驱动会抛 ValidationError
    // "不能下载目录"，被兜底映射成 500）。部分客户端会先 GET 路径做探测。
    if (fileInfo && fileInfo.isDirectory) {
      return new Response("Method Not Allowed: GET on a collection", {
        status: 405,
        headers: {
          Allow: "OPTIONS, PROPFIND, PUT, DELETE, MKCOL, COPY, MOVE, LOCK, UNLOCK, PROPPATCH",
          "Content-Type": "text/plain; charset=utf-8",
        },
      });
    }

    // 从文件信息中提取元数据
    const etag = fileInfo.etag ? `"${fileInfo.etag}"` : "";
    const lastModified = fileInfo.modified ? new Date(fileInfo.modified) : new Date();
    const lastModifiedStr = lastModified.toUTCString();
    const contentLength = fileInfo.size || 0;

    // 检查ETag匹配（如果提供了If-None-Match头）
    if (ifNoneMatch && etag) {
      // 移除引号以进行比较
      const clientEtag = ifNoneMatch.replace(/^"(.*)"$/, "$1");
      const serverEtag = etag.replace(/^"(.*)"$/, "$1");

      if (clientEtag === serverEtag || clientEtag === "*") {
        console.log(`GET请求: ETag匹配 ${etag}，返回304 Not Modified`);
        return new Response(null, {
          status: 304, // Not Modified
          headers: {
            ETag: etag,
            "Last-Modified": lastModifiedStr,
            "Cache-Control": "max-age=3600",
          },
        });
      }
    }

    // 检查修改时间（如果提供了If-Modified-Since头且没有If-None-Match头或ETag不匹配）
    if (ifModifiedSince && !ifNoneMatch) {
      try {
        const modifiedSinceDate = new Date(ifModifiedSince);

        // 将时间戳向下取整到秒，因为HTTP日期不包含毫秒
        const modifiedSinceTime = Math.floor(modifiedSinceDate.getTime() / 1000) * 1000;
        const lastModifiedTime = Math.floor(lastModified.getTime() / 1000) * 1000;

        if (lastModifiedTime <= modifiedSinceTime) {
          console.log(`GET请求: 文件未修改，返回304 Not Modified`);
          return new Response(null, {
            status: 304, // Not Modified
            headers: {
              ETag: etag,
              "Last-Modified": lastModifiedStr,
              "Cache-Control": "max-age=3600",
            },
          });
        }
      } catch (dateError) {
        console.warn(`GET请求: If-Modified-Since头格式无效: ${ifModifiedSince}`);
        // 如果日期格式无效，忽略此头，继续处理请求
      }
    }

    // 处理If-Match头（确保资源匹配）
    if (ifMatch && etag) {
      const clientEtag = ifMatch.replace(/^"(.*)"$/, "$1");
      const serverEtag = etag.replace(/^"(.*)"$/, "$1");

      if (clientEtag !== "*" && clientEtag !== serverEtag) {
        console.log(`GET请求: If-Match条件不满足 ${ifMatch} != ${etag}`);
        return createWebDAVErrorResponse("资源已被修改", 412); // Precondition Failed
      }
    }

    // 处理If-Unmodified-Since头
    if (ifUnmodifiedSince) {
      try {
        const unmodifiedSinceDate = new Date(ifUnmodifiedSince);

        // 将时间戳向下取整到秒
        const unmodifiedSinceTime = Math.floor(unmodifiedSinceDate.getTime() / 1000) * 1000;
        const lastModifiedTime = Math.floor(lastModified.getTime() / 1000) * 1000;

        if (lastModifiedTime > unmodifiedSinceTime) {
          console.log(`GET请求: If-Unmodified-Since条件不满足`);
          return createWebDAVErrorResponse("资源已被修改", 412); // Precondition Failed
        }
      } catch (dateError) {
        console.warn(`GET请求: If-Unmodified-Since头格式无效: ${ifUnmodifiedSince}`);
        // 如果日期格式无效，忽略此头，继续处理请求
      }
    }

    // 根据挂载点的 webdav_policy 配置决定处理方式
    const { driver, mount, subPath } = await mountManager.getDriverByPath(path, userId, userType);

    // Windows MiniRedir：同一路径第一次走 302，之后强制走本地代理
    const miniKey = path;
    let policy = mount.webdav_policy || "native_proxy";
    if (isWindowsMiniRedirector && miniRedirTried302.has(miniKey)) {
      policy = "native_proxy";
    }

    switch (policy) {
      case "302_redirect": {
        // 策略 1：存储直链重定向（通过驱动自己的 generateDownloadUrl 能力）
        if (typeof driver?.hasCapability === "function" && driver.hasCapability(CAPABILITIES.DIRECT_LINK)) {
          try {
            console.log(`WebDAV GET - 尝试生成存储直链: ${path}`);
            const result = await driver.generateDownloadUrl(subPath, {
              path,
              mount,
              subPath,
              db,
              userId,
              userType,
              forceDownload: false,
              channel: "webdav",
            });

            const url = extractUrlFromResult(result);
            if (url) {
              console.log(`WebDAV GET - 302 重定向到存储直链: ${url}`);

              // 对 MiniRedir，记录该路径已尝试过 302，下次访问走本地代理
              if (isWindowsMiniRedirector) {
                miniRedirTried302.add(miniKey);
              }

              return new Response(null, {
                status: 302,
                headers: getStandardWebDAVHeaders({
                  customHeaders: {
                    Location: url,
                    "Cache-Control": "no-cache",
                  },
                }),
              });
            }
          } catch (error) {
            console.warn(`WebDAV GET - 生成存储直链失败，降级到本地代理:`, error?.message || error);
          }
        }

        console.log(`WebDAV GET - 驱动不支持直链生成或未返回 URL，降级到本地代理`);
        return downloadViaStreaming(mountManager, path, c, userId, userType);
      }

      case "use_proxy_url": {
        // 策略 2：基于 storage_config.url_proxy 的代理 URL 重定向
        // 新协议：优先复用 FS 链路的 url_proxy/Worker 入口生成逻辑，保持与 /api/fs/download 一致
        try {
          const repositoryFactory = c.get("repos");
          const encryptionSecret = getEncryptionSecret(c);
          const linkService = new LinkService(db, encryptionSecret, repositoryFactory);

          // WebDAV 挂载基于 FS 视图路径，复用 FS External 链路
          const storageLink = await linkService.getFsExternalLink(path, userId, userType, {
            forceDownload: false,
            request: c.req.raw,
          });

          const url = storageLink?.url || null;
          if (url) {
            console.log(`WebDAV GET - URL 代理 use_proxy_url 通过 FS 链路生成入口: ${url}`);
            return new Response(null, {
              status: 302,
              headers: getStandardWebDAVHeaders({
                customHeaders: {
                  Location: url,
                  "Cache-Control": "no-cache",
                },
              }),
            });
          }
        } catch (error) {
          console.warn(
            `WebDAV GET - 通过 FS 链路生成 url_proxy 入口失败，将降级到本地代理:`,
            error?.message || error,
          );
        }

        console.warn(`WebDAV GET - use_proxy_url 策略下未生成 url_proxy 入口，降级到本地代理`);
        return downloadViaStreaming(mountManager, path, c, userId, userType);
      }

      case "native_proxy":
      default: {
        // 策略 3：本地服务器代理（默认兜底）
        // StorageStreaming 层统一处理
        if (isHead) {
          // HEAD 请求下只返回头信息，不传输主体内容
          const headHeaders = {
            "Content-Length": String(contentLength),
            "Content-Type": contentType,
            "Last-Modified": lastModifiedStr,
            ETag: etag,
            "Accept-Ranges": "bytes",
            "Cache-Control": "max-age=3600",
          };
          const response = new Response(null, {
            status: 200,
            headers: headHeaders,
          });
          return addWebDAVHeaders(response);
        }

        return downloadViaStreaming(mountManager, path, c, userId, userType);
      }
    }
  });
}
