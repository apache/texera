/**
 * url.ts maintains common functions related to URL.
 */

/**
 * Generate a websocket URL based on a server endpoint.
 */
export function getWebsocketUrl(endpoint: string): string {
  const websocketUrl = new URL(endpoint, document.baseURI);
  // replace protocol, so that http -> ws, https -> wss
  websocketUrl.protocol = websocketUrl.protocol.replace("http", "ws");
  return websocketUrl.toString();
}
