/**
 * Enum representing the possible states of a computing unit connection.
 * Used to track the connection status of computing units in the UI.
 */
export enum ComputingUnitConnectionState {
  Running = "Running",
  Pending = "Pending",
  Disconnected = "Disconnected",
  Terminating = "Terminating",
  NoComputingUnit = "No Computing Unit",
}
