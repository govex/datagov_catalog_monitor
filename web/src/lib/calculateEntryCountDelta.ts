import type { EntryCount } from "$lib/types";

export function calculateEntryCountDelta(entryCounts: EntryCount[], rangeIndexes?: [number, number]): number | null {
    if (entryCounts.length < 2) {
        return null; // Not enough data to calculate a difference
    }

    if (!rangeIndexes) {
        rangeIndexes = [0, entryCounts.length - 1];
    }

    const firstEntry = entryCounts[rangeIndexes[0]];
    const lastEntry = entryCounts[rangeIndexes[1]];

    return lastEntry.count - firstEntry.count;
}