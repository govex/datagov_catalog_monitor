import { DateTime } from "luxon";
import type { Organization, EntryCount } from "$lib/types";

// Function to parse a date string (Assumes format: YYYYMMDDTHHMMSS)
const parseCustomDate = (dateStr: string): Date => {
    return DateTime.fromFormat(dateStr, "yyyyMMdd'T'HHmmss", { zone: "utc" }).toJSDate();
};  

// Function to load and parse JSON data
export async function loadOrganizations(): Promise<{ organizations: Organization[] }> {
    const response = await fetch('/data/organizations.json'); // Load from static file
    if (!response.ok) {
        throw new Error("Failed to load JSON data");
    }

    const rawData = await response.json();

    // Ensure proper types and coerce incoming data structure into the type model
    const organizations: Organization[] = rawData.map((org: Organization) => ({
        ...org,
        catalog_entry_counts: Object.entries(org.catalog_entry_counts)
            .map(([key, value]) => { return {date: DateTime.fromISO(key), count: value} })
            .sort((a, b) => a.date.toMillis() - b.date.toMillis()),
        resource_entry_counts: Object.entries(org.resource_entry_counts)
            .map(([key, value]) => { return {date: DateTime.fromISO(key), count: value} })
            .sort((a, b) => a.date.toMillis() - b.date.toMillis()),
    }));

    return { organizations };
}

