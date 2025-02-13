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

    // Ensure proper types and parse dates
    const organizations: Organization[] = rawData.map((org: Organization) => ({
        ...org,
        catalog_entry_counts: org.catalog_entry_counts.map((entry: EntryCount) => ({
            ...entry,
            date: parseCustomDate(entry.date) // Convert string to Date
        })),
        resource_entry_counts: org.resource_entry_counts.map((entry: EntryCount) => ({
            ...entry,
            date: parseCustomDate(entry.date) // Convert string to Date
        }))
    }));

    return { organizations };
}

