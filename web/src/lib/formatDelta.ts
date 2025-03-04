export function formatDelta (diff: number | null): string {
    if (diff === null) return "N/A"; // Handle edge case where there's no difference
    if (diff < 0) return `⬇ ${Math.abs(diff)}`; // Handle negative difference
    if (diff > 0) return `⬆ ${diff}`; // Handle positive difference
    return ''; // Handle negative difference
}