import { result } from "lodash";
import { DashboardEntry } from "./dashboard-entry";
import { SortMethod } from "./sort-method";

export interface SearchFilterParameters {
  createDateStart: Date | null;
  createDateEnd: Date | null;
  modifiedDateStart: Date | null;
  modifiedDateEnd: Date | null;
  owners: string[];
  ids: string[];
  operators: string[];
  projectIds: number[];
}

export const toQueryStrings = (
  keywords: string[],
  params: SearchFilterParameters,
  start?: number,
  count?: number,
  type?: "workflow" | "project" | "file" | null,
  orderBy?: SortMethod
): string => {
  function* getQueryParameters(): Iterable<[name: string, value: string]> {
    if (keywords) {
      for (const keyword of keywords) {
        yield ["query", keyword.trim()];
      }
    }
    const createDateStart = params.createDateStart;
    const modifiedDateStart = params.modifiedDateStart;
    const createDateEnd = params.createDateEnd;
    const modifiedDateEnd = params.modifiedDateEnd;
    if (createDateStart) yield ["createDateStart", createDateStart.toISOString().split("T")[0]];
    if (createDateEnd) yield ["createDateEnd", createDateEnd.toISOString().split("T")[0]];
    if (modifiedDateStart) yield ["modifiedDateStart", modifiedDateStart.toISOString().split("T")[0]];
    if (modifiedDateEnd) yield ["modifiedDateEnd", modifiedDateEnd.toISOString().split("T")[0]];
    for (const owner of params.owners) {
      yield ["owner", owner];
    }
    for (const id of params.ids) {
      yield ["id", id];
    }
    for (const operator of params.operators) {
      yield ["operator", operator];
    }
    for (const id of params.projectIds) {
      yield ["projectId", id.toString()];
    }
  }
  const concatenateQueryStrings = (queryStrings: ReturnType<typeof getQueryParameters>): string =>
    [
      ...queryStrings,
      ...(start ? [["start", start.toString()]] : []),
      ...(count ? [["count", count.toString()]] : []),
      ["resourceType", type ? type.toString() : ""],
      ...(orderBy ? [["orderBy", SortMethod[orderBy]]] : []),
    ]
      .filter(q => q[1])
      .map(([name, value]) => name + "=" + encodeURIComponent(value))
      .join("&");
  return concatenateQueryStrings(getQueryParameters());
};

/** JavaScript-based search function used for only unit tests. Actual search is done on the server. */
export const searchTestEntries = (
  keywords: string[],
  params: SearchFilterParameters,
  testEntries: DashboardEntry[],
  type: "workflow" | "project" | "file" | null
): DashboardEntry[] => {
  const endOfDay = (date: Date) => {
    date.setHours(23);
    date.setMinutes(59);
    date.setSeconds(59);
    date.setMilliseconds(999);
    return date.getTime();
  };
  const createDateStart = params.createDateStart;
  const modifiedDateStart = params.modifiedDateStart;
  const createDateEnd = params.createDateEnd;
  const modifiedDateEnd = params.modifiedDateEnd;
  if (keywords.length > 0) {
    testEntries = testEntries.filter(e => keywords.some(k => e.name.indexOf(k) !== -1));
  }
  if (createDateStart) {
    testEntries = testEntries.filter(e => e.creationTime && e.creationTime >= createDateStart.getTime());
  }
  if (createDateEnd) {
    testEntries = testEntries.filter(e => e.creationTime && e.creationTime <= endOfDay(createDateEnd));
  }
  if (modifiedDateStart) {
    testEntries = testEntries.filter(e => e.lastModifiedTime && e.lastModifiedTime >= modifiedDateStart.getTime());
  }
  if (modifiedDateEnd) {
    testEntries = testEntries.filter(e => e.lastModifiedTime && e.lastModifiedTime <= endOfDay(modifiedDateEnd));
  }
  if (params.owners.length > 0) {
    testEntries = testEntries.filter(e => params.owners.some(o => e.type === "workflow" && e.workflow.ownerName === o));
  }
  if (params.ids.length > 0) {
    testEntries = testEntries.filter(e =>
      params.ids.some(i => e.type === "workflow" && e.workflow.workflow.wid && e.workflow.workflow.wid.toString() === i)
    );
  }
  if (params.operators.length > 0) {
    testEntries = testEntries.filter(
      e =>
        e.type === "workflow" &&
        e.workflow.workflow.content.operators.some(operator =>
          params.operators.some(operatorTypeFilterBy => operatorTypeFilterBy === operator.operatorType)
        )
    );
  }
  if (params.projectIds.length > 0) {
    testEntries = testEntries.filter(
      e =>
        e.type === "workflow" &&
        e.workflow.projectIDs.some(id => params.projectIds.some(projectIdToFilterBy => projectIdToFilterBy == id))
    );
  }
  if (type) {
    testEntries = testEntries.filter(e => e.type === type);
  }
  return testEntries;
};
