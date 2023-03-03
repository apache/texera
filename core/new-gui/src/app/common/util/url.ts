/**
 * url.ts maintains common functions related to URL.
 */

import normalizeUrl from "normalize-url";
/**
 * Generate a websocket URL based on a server endpoint.
 */
export function getWebsocketUrl(endpoint: string): string {
  const websocketUrl = new URL(endpoint, document.baseURI);
  // replace protocol, so that http -> ws, https -> wss
  websocketUrl.protocol = websocketUrl.protocol.replace("http", "ws");
  return websocketUrl.toString();
}
/**
 * Generate a URL based on hostname, port and path.
 */
export function createUrl(hostname: string, port: number, path: string): string {
  const protocol = location.protocol === "https:" ? "wss" : "ws";
  return normalizeUrl(`${protocol}://${hostname}:${port}${path}`);
}
