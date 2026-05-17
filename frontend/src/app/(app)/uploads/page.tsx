"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowsClockwiseIcon, ClockCounterClockwiseIcon, DatabaseIcon } from "@phosphor-icons/react";
import { AppShell } from "@/components/app-shell";
import { UploadsHistorySection } from "@/components/uploads/uploads-history-section";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import type { ClaimSyncStateSummary, UploadDataset, UploadRecord } from "@/lib/uploads/types";

export default function UploadsPage() {
  const queryClient = useQueryClient();

  const datasetsQuery = useQuery({
    queryKey: ["upload-datasets"],
    queryFn: async () => {
      const response = await fetch("/api/uploads/datasets");
      if (!response.ok) throw new Error("Failed to load upload datasets");
      return response.json() as Promise<{ datasets: UploadDataset[] }>;
    },
  });

  const uploadsQuery = useQuery({
    queryKey: ["recent-uploads"],
    queryFn: async () => {
      const response = await fetch("/api/uploads");
      if (!response.ok) throw new Error("Failed to load recent uploads");
      return response.json() as Promise<{ uploads: UploadRecord[] }>;
    },
  });

  const syncStateQuery = useQuery({
    queryKey: ["claim-sync-state"],
    queryFn: async () => {
      const response = await fetch("/api/claims/sync-state");
      if (!response.ok) throw new Error("Failed to load claim sync state");
      return response.json() as Promise<{ syncState: ClaimSyncStateSummary | null }>;
    },
  });

  const syncState = syncStateQuery.data?.syncState ?? null;

  return (
    <AppShell>
      <div className="max-w-6xl space-y-6 p-6">
        <header className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="type-headline">Uploads</h1>
            <p className="type-caption text-muted-foreground">
              Review landed files, GCS generations, and the latest claim discovery sync state.
            </p>
          </div>
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              queryClient.invalidateQueries({ queryKey: ["recent-uploads"] });
              queryClient.invalidateQueries({ queryKey: ["claim-sync-state"] });
            }}
          >
            <ArrowsClockwiseIcon data-icon="inline-start" />
            Refresh
          </Button>
        </header>

        <section className="border border-border p-4">
          <div className="flex items-center gap-2">
            <DatabaseIcon className="size-4 text-muted-foreground" />
            <h2 className="type-title">Latest claim sync</h2>
          </div>
          {syncStateQuery.isLoading ? (
            <div className="mt-3 space-y-2">
              <Skeleton className="h-4 w-48" />
              <Skeleton className="h-4 w-64" />
            </div>
          ) : syncState ? (
            <div className="mt-4 grid gap-3 md:grid-cols-4">
              <SyncMetric label="Discovered" value={String(syncState.lastDiscoveredCount)} />
              <SyncMetric label="Inserted" value={String(syncState.lastInsertedCount)} />
              <SyncMetric label="Last claim" value={syncState.lastClaimId ?? "—"} />
              <SyncMetric label="Synced at" value={new Date(syncState.lastSyncedAt).toLocaleString()} />
            </div>
          ) : (
            <div className="mt-3 flex items-center gap-2 text-sm text-muted-foreground">
              <ClockCounterClockwiseIcon className="size-4" />
              <span>No claim sync has been recorded yet.</span>
            </div>
          )}
        </section>

        <UploadsHistorySection
          datasets={datasetsQuery.data?.datasets ?? []}
          isLoading={uploadsQuery.isLoading || datasetsQuery.isLoading}
          title="Recent uploads"
          uploads={uploadsQuery.data?.uploads ?? []}
        />
      </div>
    </AppShell>
  );
}

function SyncMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <p className="type-label text-muted-foreground">{label}</p>
      <p className="truncate font-mono text-xs text-foreground">{value}</p>
    </div>
  );
}
