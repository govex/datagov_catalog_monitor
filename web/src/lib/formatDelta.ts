export function formatDelta (diff: number | null): string {
    if (diff === null) return "N/A"; // Handle edge case where there's no difference
    return diff >= 0 ? `⬆️ ${diff}` : `${diff}`; // Add "+" for positive numbers
}