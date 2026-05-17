"use client";

import { useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import {
  DatabaseIcon,
  FileCsvIcon,
  FilePdfIcon,
  UploadSimpleIcon,
  WarningIcon,
} from "@phosphor-icons/react";
import { AppShell } from "@/components/app-shell";
import { UploadsHistorySection } from "@/components/uploads/uploads-history-section";
import { StatusMetric } from "@/components/uploads/status-metric";
import { UploadFileRow } from "@/components/uploads/upload-file-row";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { UploadDatasetKey } from "@/lib/uploads/registry";
import type {
  ClaimSyncStateSummary,
  SelectedUpload,
  UploadDataset,
  UploadRecord,
} from "@/lib/uploads/types";
import { formatBytes, validateFile, uploadOne } from "@/lib/uploads/upload-helpers";

export default function DataUploadPage() {
  const queryClient = useQueryClient();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [datasetKey, setDatasetKey] = useState<UploadDatasetKey>("claims");
  const [files, setFiles] = useState<SelectedUpload[]>([]);
  const [isDragActive, setIsDragActive] = useState(false);

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
      return response.json() as Promise<{
        syncState: ClaimSyncStateSummary | null;
      }>;
    },
  });

  const datasets = datasetsQuery.data?.datasets ?? [];
  const selectedDataset =
    datasets.find((dataset) => dataset.datasetKey === datasetKey) ??
    datasets[0];
  const hasInvalidFiles = files.some((file) => file.status === "invalid");
  const readyFiles = files.filter((file) => file.status === "selected");
  const isBusy = files.some((file) =>
    ["signing", "uploading", "verifying"].includes(file.status),
  );
  const detectedHeaders =
    files.find((file) => file.headers?.length)?.headers ?? [];
  const latestLandedFile = [...files]
    .reverse()
    .find((file) => file.status === "landed" && file.volumePath);

  const landingSummary = useMemo(() => {
    if (!selectedDataset) return "Select a dataset to see its landing prefix.";
    return `/Volumes/healthcare/bronze/raw_landing/${selectedDataset.landingSubdirectory}/`;
  }, [selectedDataset]);

  const uploadMutation = useMutation({
    mutationFn: async () => {
      if (!selectedDataset) return;
      let failed = 0;
      let landed = 0;

      for (const item of readyFiles) {
        const result = await uploadOne(item, selectedDataset, setFiles);
        if (result === "failed") failed += 1;
        if (result === "ok") landed += 1;
      }

      if (failed > 0) {
        toast.error(`${failed} upload${failed === 1 ? "" : "s"} failed`);
      }
      if (landed > 0) {
        toast.success("Files landed — processing starts within a few minutes");
      }
      await queryClient.invalidateQueries({ queryKey: ["recent-uploads"] });
      await queryClient.invalidateQueries({ queryKey: ["claim-sync-state"] });
    },
  });

  async function onFilesSelected(fileList: FileList | null) {
    if (!selectedDataset || !fileList) return;
    const nextFiles: SelectedUpload[] = [];

    for (const file of Array.from(fileList)) {
      nextFiles.push(await validateFile(file, selectedDataset));
    }

    setFiles(nextFiles);
  }

  function onDragOver(e: React.DragEvent) {
    e.preventDefault();
    if (!selectedDataset || isBusy) return;
    setIsDragActive(true);
  }

  function onDragLeave(e: React.DragEvent) {
    if (!e.currentTarget.contains(e.relatedTarget as Node))
      setIsDragActive(false);
  }

  async function onDrop(e: React.DragEvent) {
    e.preventDefault();
    setIsDragActive(false);
    if (!selectedDataset || isBusy) return;
    await onFilesSelected(e.dataTransfer.files);
  }

  return (
    <AppShell>
      <div className="max-w-6xl space-y-6 p-6">
        <header className="space-y-2">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h1 className="type-headline">Data Upload</h1>
              <p className="type-caption text-muted-foreground">
                Upload claim files for processing. Files are picked up
                automatically within 5–10 minutes.
              </p>
            </div>
            <Tooltip>
              <TooltipTrigger render={<span />} className="inline-flex">
                <Badge variant="outline" className="gap-1 cursor-help">
                  <DatabaseIcon data-icon="inline-start" />
                  Bronze landing
                </Badge>
              </TooltipTrigger>
              <TooltipContent side="bottom">
                Files land in the Bronze layer of the Delta Lake medallion
                architecture. ETL processes them into Silver and Gold tables.
              </TooltipContent>
            </Tooltip>
          </div>

          <div className="flex items-center gap-4 border-y border-border py-2.5">
            <StatusMetric label="Upload path" value={landingSummary} />
          </div>
        </header>

        <section className="grid gap-6 lg:grid-cols-[280px_1fr]">
          <div className="space-y-3">
            <p className="type-label text-muted-foreground">Dataset</p>
            {datasetsQuery.isLoading && (
              <div className="space-y-2">
                {Array.from({ length: 3 }).map((_, index) => (
                  <Skeleton key={index} className="h-16 w-full" />
                ))}
              </div>
            )}
            {datasets.map((dataset) => (
              <button
                key={dataset.datasetKey}
                type="button"
                onClick={() => {
                  setDatasetKey(dataset.datasetKey);
                  setFiles([]);
                }}
                className={`w-full border px-3 py-3 text-left transition-colors ${
                  dataset.datasetKey === datasetKey
                    ? "border-foreground bg-muted/60"
                    : "border-border hover:bg-muted/50"
                }`}
              >
                <div className="flex items-center gap-2">
                  {dataset.extension === ".csv" ? (
                    <FileCsvIcon className="size-4" />
                  ) : (
                    <FilePdfIcon className="size-4" />
                  )}
                  <span className="text-sm font-medium">
                    {dataset.displayName}
                  </span>
                  {dataset.hasPhi && (
                    <Badge variant="destructive" className="ml-auto">
                      PHI
                    </Badge>
                  )}
                </div>
                <p className="mt-1 line-clamp-2 text-xs text-muted-foreground">
                  {dataset.description}
                </p>
              </button>
            ))}
          </div>

          <div className="space-y-5">
            <section
              className={`space-y-4 border border-border p-4 transition-colors${isDragActive ? " bg-muted/40" : ""}`}
              onDragOver={onDragOver}
              onDragLeave={onDragLeave}
              onDrop={onDrop}
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <h2 className="type-title">
                    {selectedDataset?.displayName ?? "Upload"}
                  </h2>
                  <p className="type-caption text-muted-foreground">
                    {selectedDataset?.extension.toUpperCase()} only, max{" "}
                    {formatBytes(selectedDataset?.maxBytes ?? 0)} per file.
                  </p>
                </div>
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => fileInputRef.current?.click()}
                  disabled={!selectedDataset || isBusy}
                >
                  <UploadSimpleIcon data-icon="inline-start" />
                  Select files
                </Button>
                <Input
                  ref={fileInputRef}
                  type="file"
                  multiple
                  accept={
                    selectedDataset?.extension === ".csv"
                      ? ".csv,text/csv"
                      : ".pdf,application/pdf"
                  }
                  className="hidden"
                  onChange={(event) => onFilesSelected(event.target.files)}
                />
              </div>

              {selectedDataset?.requiredColumns.length ? (
                <div className="space-y-2">
                  <p className="type-label text-muted-foreground">
                    Required CSV columns
                  </p>
                  <div className="flex flex-wrap gap-1.5">
                    {selectedDataset.requiredColumns.map((column) => {
                      const present =
                        detectedHeaders.length === 0 ||
                        detectedHeaders.includes(column);
                      return (
                        <Badge
                          key={column}
                          variant={present ? "outline" : "destructive"}
                          className="font-mono"
                        >
                          {column}
                        </Badge>
                      );
                    })}
                  </div>
                </div>
              ) : null}

              {detectedHeaders.length > 0 &&
              selectedDataset?.extension === ".csv" ? (
                <div className="space-y-2">
                  <p className="type-label text-muted-foreground">
                    Detected columns
                  </p>
                  <div className="flex flex-wrap gap-1.5">
                    {detectedHeaders.map((column) => {
                      const isRequired =
                        selectedDataset.requiredColumns.includes(column);
                      return (
                        <Badge
                          key={column}
                          variant={isRequired ? "outline" : "secondary"}
                          className="font-mono"
                        >
                          {column}
                        </Badge>
                      );
                    })}
                    {selectedDataset.requiredColumns
                      .filter((column) => !detectedHeaders.includes(column))
                      .map((column) => (
                        <Badge
                          key={column}
                          variant="destructive"
                          className="font-mono"
                        >
                          {column}
                        </Badge>
                      ))}
                  </div>
                </div>
              ) : null}

              {selectedDataset?.hasPhi && (
                <Alert>
                  <WarningIcon className="size-4" />
                  <AlertTitle>PHI handling</AlertTitle>
                  <AlertDescription>
                    CSV object names use generated IDs only. This page reads
                    only the header row for validation.
                  </AlertDescription>
                </Alert>
              )}

              {files.length === 0 ? (
                <Empty className="border border-dashed border-border py-12">
                  <EmptyHeader>
                    <EmptyMedia variant="icon">
                      <UploadSimpleIcon />
                    </EmptyMedia>
                    <EmptyTitle>No files selected</EmptyTitle>
                    <EmptyDescription>
                      Drop files here, or use Select files above.
                    </EmptyDescription>
                  </EmptyHeader>
                </Empty>
              ) : (
                <div className="space-y-3">
                  {files.map((item) => (
                    <UploadFileRow
                      key={`${item.file.name}-${item.file.size}`}
                      item={item}
                      onRemove={() =>
                        setFiles((f) => f.filter((x) => x.file !== item.file))
                      }
                      onCancel={() => {
                        item.controller?.abort();
                        setFiles((f) =>
                          f.map((x) =>
                            x.file === item.file
                              ? { ...x, status: "cancelled" }
                              : x,
                          ),
                        );
                      }}
                    />
                  ))}
                </div>
              )}

              {files.length > 0 &&
                files.every((f) => f.status === "landed") && (
                  <p className="type-caption text-muted-foreground border-t border-border pt-3">
                    Files queued for ETL pickup. New claims typically appear in
                    the queue within 5–10 minutes.
                  </p>
                )}

              {latestLandedFile && (
                <section className="border border-border p-4">
                  <h3 className="type-title">Upload completed</h3>
                  <div className="mt-3 grid gap-3 md:grid-cols-2">
                    <StatusMetric
                      label="Dataset"
                      value={selectedDataset?.displayName ?? datasetKey}
                    />
                    <StatusMetric
                      label="Generation"
                      value={latestLandedFile.gcsGeneration ?? "—"}
                    />
                    <StatusMetric
                      label="Volume path"
                      value={latestLandedFile.volumePath ?? "—"}
                    />
                    <StatusMetric
                      label="Sync status"
                      value="Waiting for ETL trigger"
                    />
                  </div>
                  <p className="type-caption text-muted-foreground mt-3">
                    Databricks waits for the file-arrival debounce window before
                    ETL begins.
                  </p>
                  {syncStateQuery.data?.syncState && (
                    <div className="mt-3 grid gap-3 md:grid-cols-4">
                      <StatusMetric
                        label="Discovered"
                        value={String(
                          syncStateQuery.data.syncState.lastDiscoveredCount,
                        )}
                      />
                      <StatusMetric
                        label="Inserted"
                        value={String(
                          syncStateQuery.data.syncState.lastInsertedCount,
                        )}
                      />
                      <StatusMetric
                        label="Last claim"
                        value={syncStateQuery.data.syncState.lastClaimId ?? "—"}
                      />
                      <StatusMetric
                        label="Synced at"
                        value={new Date(
                          syncStateQuery.data.syncState.lastSyncedAt,
                        ).toLocaleString()}
                      />
                    </div>
                  )}
                </section>
              )}

              <div className="flex flex-wrap justify-end gap-2 border-t border-border pt-4">
                <Button
                  type="button"
                  variant="ghost"
                  onClick={() => setFiles([])}
                  disabled={files.length === 0 || isBusy}
                >
                  Clear
                </Button>
                <Button
                  type="button"
                  onClick={() => uploadMutation.mutate()}
                  disabled={
                    readyFiles.length === 0 || hasInvalidFiles || isBusy
                  }
                >
                  <UploadSimpleIcon data-icon="inline-start" />
                  Upload {readyFiles.length > 0 ? readyFiles.length : ""}
                </Button>
              </div>
            </section>

            <UploadsHistorySection
              datasets={datasets}
              isLoading={uploadsQuery.isLoading}
              uploads={uploadsQuery.data?.uploads ?? []}
            />
          </div>
        </section>
      </div>
    </AppShell>
  );
}
