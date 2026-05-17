import {
  CheckCircleIcon,
  DatabaseIcon,
  WarningIcon,
  XCircleIcon,
} from "@phosphor-icons/react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { UploadDataset, UploadRecord } from "@/lib/uploads/types";

export function UploadsHistorySection({
  datasets,
  isLoading,
  title = "Recent Uploads",
  uploads,
}: {
  datasets: UploadDataset[];
  isLoading: boolean;
  title?: string;
  uploads: UploadRecord[];
}) {
  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="type-title">{title}</h2>
        <span className="type-caption text-muted-foreground">Last 25</span>
      </div>
      <Alert>
        <WarningIcon className="size-4" />
        <AlertTitle>Ingestion timing</AlertTitle>
        <AlertDescription>
          Databricks waits 60 seconds after the last file change and enforces a 300 second minimum between file-arrival triggers, so ETL is not immediate.
        </AlertDescription>
      </Alert>
      {isLoading ? (
        <div className="space-y-2" role="status" aria-label="Loading uploads">
          {Array.from({ length: 4 }).map((_, index) => (
            <Skeleton key={index} className="h-10 w-full" />
          ))}
        </div>
      ) : uploads.length === 0 ? (
        <Empty className="border border-border py-10">
          <EmptyContent>
            <EmptyMedia variant="icon">
              <DatabaseIcon />
            </EmptyMedia>
            <EmptyTitle>No upload history</EmptyTitle>
            <EmptyDescription>Landed files will appear here after verification.</EmptyDescription>
          </EmptyContent>
        </Empty>
      ) : (
        <div className="border border-border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Upload ID</TableHead>
                <TableHead>Dataset</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Uploaded by</TableHead>
                <TableHead>Size</TableHead>
                <TableHead>Created</TableHead>
                <TableHead>Completed</TableHead>
                <TableHead>Generation</TableHead>
                <TableHead>Error</TableHead>
                <TableHead>Volume path</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {uploads.map((upload) => (
                <TableRow key={upload.id}>
                  <TableCell className="font-mono text-xs">{upload.id}</TableCell>
                  <TableCell className="font-medium">
                    {datasets.find((d) => d.datasetKey === upload.datasetKey)?.displayName ?? upload.datasetKey}
                  </TableCell>
                  <TableCell>
                    <UploadStatusBadge status={upload.status} />
                  </TableCell>
                  <TableCell className="max-w-[180px] truncate text-xs text-muted-foreground">
                    {upload.uploadedByEmail}
                  </TableCell>
                  <TableCell>{formatBytes(upload.byteSize)}</TableCell>
                  <TableCell>{formatDateTime(upload.createdAt)}</TableCell>
                  <TableCell>{formatDateTime(upload.completedAt)}</TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {upload.gcsGeneration ?? "—"}
                  </TableCell>
                  <TableCell className="max-w-[220px] text-xs text-muted-foreground">
                    {upload.errorMessage ?? "—"}
                  </TableCell>
                  <TableCell className="max-w-[320px] truncate font-mono text-xs text-muted-foreground">
                    {upload.volumePath}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </section>
  );
}

function UploadStatusBadge({ status }: { status: UploadRecord["status"] }) {
  if (status === "uploaded") {
    return (
      <Badge>
        <CheckCircleIcon data-icon="inline-start" />
        Landed
      </Badge>
    );
  }
  if (status === "failed") {
    return (
      <Badge variant="destructive">
        <XCircleIcon data-icon="inline-start" />
        Failed
      </Badge>
    );
  }
  return <Badge variant="outline">Initiated</Badge>;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
}

function formatDateTime(value: string | null): string {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}
