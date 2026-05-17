import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { FileCsvIcon, FilePdfIcon, ProhibitIcon, XIcon } from "@phosphor-icons/react";
import type { SelectedUpload } from "@/lib/uploads/types";
import { formatBytes } from "@/lib/uploads/upload-helpers";
import { STATUS_COPY } from "@/lib/uploads/types";

export function UploadFileRow({
  item,
  onRemove,
  onCancel,
}: {
  item: SelectedUpload;
  onRemove: () => void;
  onCancel: () => void;
}) {
  const isError = item.status === "invalid" || item.status === "failed";
  const canRemove = !["signing", "uploading", "verifying", "landed"].includes(
    item.status,
  );
  const canCancel = ["signing", "uploading", "verifying"].includes(item.status);
  return (
    <div className="space-y-2 border border-border px-3 py-3">
      <div className="flex min-w-0 items-center gap-3">
        {item.file.type === "application/pdf" ? (
          <FilePdfIcon className="size-4 shrink-0 text-muted-foreground" />
        ) : (
          <FileCsvIcon className="size-4 shrink-0 text-muted-foreground" />
        )}
        <div className="min-w-0 flex-1">
          <p className="truncate text-sm font-medium">{item.file.name}</p>
          <p className="type-caption text-muted-foreground">
            {formatBytes(item.file.size)}
          </p>
        </div>
        <Badge
          variant={
            isError
              ? "destructive"
              : item.status === "landed"
                ? "default"
                : "outline"
          }
        >
          {STATUS_COPY[item.status]}
        </Badge>
        {canRemove && (
          <Button
            type="button"
            variant="ghost"
            size="icon"
            onClick={onRemove}
            aria-label="Remove file"
          >
            <XIcon className="size-4" />
          </Button>
        )}
        {canCancel && (
          <Button
            type="button"
            variant="ghost"
            size="icon"
            onClick={onCancel}
            aria-label="Cancel upload"
          >
            <ProhibitIcon className="size-4" />
          </Button>
        )}
      </div>
      {item.status !== "selected" &&
        item.status !== "invalid" &&
        item.status !== "cancelled" && <Progress value={item.progress} />}
      {item.error && (
        <p className="type-caption text-destructive">{item.error}</p>
      )}
      {item.volumePath && item.status === "landed" && (
        <p className="type-caption text-muted-foreground">
          Queued for ETL pickup.
        </p>
      )}
    </div>
  );
}
