/**
 * Used to extract error from the backend-thrown error
 * @param err
 */
export function extractErrorMessage(err: unknown): string {
  if (err instanceof Error) {
    return err.message;
  }

  if (typeof err === "object" && err !== null && "error" in err) {
    const backendErr = (err as any).error;
    if (typeof backendErr === "string") {
      return backendErr;
    }
    if (typeof backendErr === "object" && "message" in backendErr) {
      return backendErr.message;
    }
  }

  return "An unknown error occurred.";
}
