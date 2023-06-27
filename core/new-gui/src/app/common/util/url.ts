/**
 * url.ts maintains common functions related to URL.
 */

/**
 * Generate a websocket URL based on a server endpoint.
 */
export function getWebsocketUrl(endpoint: string, port: string): string {
  const baseURI = document.baseURI;
  const hostname = new URL(baseURI).hostname;
  let webSocketUrl;
  if (port !== "") {
    webSocketUrl = new URL(endpoint, `http://${hostname}:${port}`);
  } else {
    webSocketUrl = new URL(endpoint, document.baseURI);
  }

  // replace protocol, so that http -> ws, https -> wss
  webSocketUrl.protocol = webSocketUrl.protocol.replace("http", "ws");
  return webSocketUrl.toString();
}
