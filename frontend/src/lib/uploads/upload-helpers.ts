import { toast } from "sonner";
import type { UploadDataset } from "@/lib/uploads/types";
import type {
  InitiateUploadResponse,
  SelectedUpload,
  SignedPolicy,
} from "@/lib/uploads/types";

async function readCsvHeaders(file: File): Promise<string[]> {
  const text = await file.slice(0, 64 * 1024).text();
  const headerLine = (text.split(/\r?\n/, 1)[0] ?? "").replace(/^\uFEFF/, "");
  return headerLine
    .split(",")
    .map((header) => header.trim().replace(/^"|"$/g, ""))
    .filter(Boolean);
}

export function formatBytes(bytes: number) {
  if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(1)} MB`;
  if (bytes >= 1_000) return `${(bytes / 1_000).toFixed(1)} KB`;
  return `${bytes} B`;
}

export function fallbackContentType(extension: UploadDataset["extension"]) {
  return extension === ".pdf" ? "application/pdf" : "text/csv";
}

export async function validateFile(
  file: File,
  dataset: UploadDataset,
): Promise<SelectedUpload> {
  if (!file.name.toLowerCase().endsWith(dataset.extension)) {
    return {
      file,
      progress: 0,
      status: "invalid",
      error: `Expected ${dataset.extension} file`,
    };
  }
  if (file.size > dataset.maxBytes) {
    return {
      file,
      progress: 0,
      status: "invalid",
      error: `File exceeds ${formatBytes(dataset.maxBytes)}`,
    };
  }

  if (dataset.extension === ".csv") {
    const headers = await readCsvHeaders(file);
    const missingColumns = dataset.requiredColumns.filter(
      (column) => !headers.includes(column),
    );
    if (missingColumns.length > 0) {
      return {
        file,
        headers,
        missingColumns,
        progress: 0,
        status: "invalid",
        error: `Missing columns: ${missingColumns.join(", ")}`,
      };
    }
    return { file, headers, progress: 0, status: "selected" };
  }

  return { file, progress: 0, status: "selected" };
}

function updateFile(
  file: File,
  setFiles: React.Dispatch<React.SetStateAction<SelectedUpload[]>>,
  patch: Partial<SelectedUpload>,
) {
  setFiles((current) =>
    current.map((item) =>
      item.file === file
        ? {
            ...item,
            ...patch,
          }
        : item,
    ),
  );
}

function throwFileError(
  file: File,
  setFiles: React.Dispatch<React.SetStateAction<SelectedUpload[]>>,
  message: string,
) {
  updateFile(file, setFiles, { error: message, status: "failed" });
  toast.error(message);
}

function postToGcs(
  policy: SignedPolicy,
  file: File,
  signal: AbortSignal,
  onProgress: (value: number) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();
    const formData = new FormData();
    for (const [key, value] of Object.entries(policy.fields)) {
      formData.append(key, value);
    }
    formData.append("file", file);

    request.upload.onprogress = (event) => {
      if (event.lengthComputable) {
        onProgress(10 + Math.round((event.loaded / event.total) * 80));
      }
    };
    request.upload.onabort = () => reject(new Error("Upload cancelled"));
    request.onload = () => {
      if (request.status >= 200 && request.status < 300) {
        resolve();
      } else {
        reject(new Error("GCS rejected the upload"));
      }
    };
    request.onerror = () => reject(new Error("GCS upload failed"));
    signal.addEventListener("abort", () => request.abort());
    request.open("POST", policy.url);
    request.send(formData);
  });
}

export async function uploadOne(
  item: SelectedUpload,
  dataset: UploadDataset,
  setFiles: React.Dispatch<React.SetStateAction<SelectedUpload[]>>,
): Promise<"ok" | "failed" | "cancelled"> {
  const { file, headers } = item;
  const controller = new AbortController();
  updateFile(file, setFiles, {
    status: "signing",
    progress: 5,
    error: undefined,
    controller,
  });
  try {
    const initiateResponse = await fetch("/api/uploads/initiate", {
      body: JSON.stringify({
        byteSize: file.size,
        contentType: file.type || fallbackContentType(dataset.extension),
        datasetKey: dataset.datasetKey,
        fileName: file.name,
        headers,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
      signal: controller.signal,
    });

    const initiatePayload = (await initiateResponse.json()) as
      | InitiateUploadResponse
      | { error?: string };
    if (!initiateResponse.ok || !("policy" in initiatePayload)) {
      const message =
        "error" in initiatePayload && initiatePayload.error
          ? initiatePayload.error
          : "Could not sign upload";
      throwFileError(file, setFiles, message);
      return "failed";
    }

    updateFile(file, setFiles, {
      progress: 10,
      status: "uploading",
      uploadId: initiatePayload.uploadId,
      volumePath: initiatePayload.volumePath,
    });

    await postToGcs(
      initiatePayload.policy,
      file,
      controller.signal,
      (progress) => {
        updateFile(file, setFiles, {
          progress: Math.max(10, Math.min(90, progress)),
        });
      },
    );

    updateFile(file, setFiles, { progress: 95, status: "verifying" });
    const completeResponse = await fetch("/api/uploads/complete", {
      body: JSON.stringify({ uploadId: initiatePayload.uploadId }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    });
    const completePayload = (await completeResponse
      .json()
      .catch(() => ({}))) as {
      error?: string;
      gcsGeneration?: string | null;
    };

    if (!completeResponse.ok) {
      throwFileError(
        file,
        setFiles,
        completePayload.error ?? "Upload verification failed",
      );
      return "failed";
    }

    updateFile(file, setFiles, {
      gcsGeneration: completePayload.gcsGeneration ?? undefined,
      progress: 100,
      status: "landed",
    });
    return "ok";
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      updateFile(file, setFiles, {
        status: "cancelled",
        progress: 0,
        error: undefined,
      });
      return "cancelled";
    }
    throwFileError(
      file,
      setFiles,
      error instanceof Error ? error.message : "Upload failed",
    );
    return "failed";
  }
}
