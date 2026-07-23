import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";

const ISO_INSTANT =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|([+-])(\d{2}):(\d{2}))$/;

const isLeapYear = (year: number) => year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);

const daysInMonth = (year: number, month: number) =>
  [31, isLeapYear(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1] ?? 0;

export interface AgentControlTaskSourceTimestamp {
  readonly epochMilliseconds: number;
  readonly canonical: string;
}

export const parseAgentControlTaskSourceTimestamp = (
  value: string,
): AgentControlTaskSourceTimestamp | null => {
  const match = ISO_INSTANT.exec(value);
  if (match === null) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const offsetHour = match[8] === undefined ? 0 : Number(match[8]);
  const offsetMinute = match[9] === undefined ? 0 : Number(match[9]);
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > daysInMonth(year, month) ||
    hour > 23 ||
    minute > 59 ||
    second > 59 ||
    offsetHour > 23 ||
    offsetMinute > 59
  ) {
    return null;
  }
  const parsed = DateTime.make(value);
  if (Option.isNone(parsed)) return null;
  return {
    epochMilliseconds: parsed.value.epochMilliseconds,
    canonical: DateTime.formatIso(parsed.value),
  };
};

export const compareAgentControlTaskSourceTimestamps = (
  left: string,
  right: string,
): -1 | 0 | 1 | null => {
  const leftTimestamp = parseAgentControlTaskSourceTimestamp(left);
  const rightTimestamp = parseAgentControlTaskSourceTimestamp(right);
  if (leftTimestamp === null || rightTimestamp === null) return null;
  if (leftTimestamp.epochMilliseconds < rightTimestamp.epochMilliseconds) return -1;
  if (leftTimestamp.epochMilliseconds > rightTimestamp.epochMilliseconds) return 1;
  return 0;
};

export const canonicalAgentControlTaskSourceTimestamp = (value: string): string | null =>
  parseAgentControlTaskSourceTimestamp(value)?.canonical ?? null;
