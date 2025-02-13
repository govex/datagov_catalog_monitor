import type { EntryCount } from "$lib/types";

export function calculateEntryCountDelta(entryCounts: EntryCount[]): number | null {
    if (entryCounts.length < 2) {
        return null; // Not enough data to calculate a difference
    }

    const firstEntry = entryCounts[0];
    const lastEntry = entryCounts[entryCounts.length - 1];

    return lastEntry.count - firstEntry.count;
}