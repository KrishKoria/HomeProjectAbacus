"use client";

import { useSyncExternalStore } from "react";
import { useQueryClient } from "@tanstack/react-query";

export interface AnalysisQueueItem {
  claimId: string;
  priority: number;
}

export interface AnalysisQueueError {
  claimId: string;
  message: string;
}

interface QueueSnapshot {
  isProcessing: boolean;
  progress: {
    completed: number;
    current: string | null;
    errors: AnalysisQueueError[];
    failed: number;
    total: number;
  };
}

interface UseAnalysisQueueReturn extends QueueSnapshot {
  enqueue: (claimId: string, priority: number) => void;
  enqueueBatch: (claimIds: string[], priority: number) => void;
  reset: () => void;
}

const listeners = new Set<() => void>();
const processedClaimIds = new Set<string>();
const queuedClaimIds = new Set<string>();
const queue: AnalysisQueueItem[] = [];

let currentClaimId: string | null = null;
let completedCount = 0;
let failedCount = 0;
let totalCount = 0;
let queueErrors: AnalysisQueueError[] = [];
let isProcessingQueue = false;
let snapshot: QueueSnapshot = {
  isProcessing: false,
  progress: {
    completed: 0,
    current: null,
    errors: [],
    failed: 0,
    total: 0,
  },
};

function refreshSnapshot() {
  snapshot = {
    isProcessing: isProcessingQueue,
    progress: {
      completed: completedCount,
      current: currentClaimId,
      errors: queueErrors,
      failed: failedCount,
      total: totalCount,
    },
  };
}

function notify() {
  refreshSnapshot();
  listeners.forEach((listener) => listener());
}

function getSnapshot(): QueueSnapshot {
  return snapshot;
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

function resetQueueState() {
  queue.length = 0;
  queuedClaimIds.clear();
  processedClaimIds.clear();
  currentClaimId = null;
  completedCount = 0;
  failedCount = 0;
  totalCount = 0;
  queueErrors = [];
  isProcessingQueue = false;
  notify();
}

async function analyzeClaim(
  claimId: string,
  invalidate: (claimId: string) => Promise<void>,
): Promise<{ ok: true } | { message: string; ok: false }> {
  try {
    const response = await fetch("/api/claims/analyze", {
      body: JSON.stringify({ claimId }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    });

    if (!response.ok) {
      const payload = (await response.json().catch(() => null)) as { error?: string } | null;
      return {
        message: payload?.error ?? `Analysis failed with status ${response.status}`,
        ok: false,
      };
    }

    await invalidate(claimId);
    return { ok: true };
  } catch (error) {
    return {
      message: error instanceof Error ? error.message : "Analysis request failed",
      ok: false,
    };
  }
}

async function processQueue(invalidate: (claimId: string) => Promise<void>) {
  if (isProcessingQueue) return;

  isProcessingQueue = true;
  notify();

  while (queue.length > 0) {
    queue.sort((left, right) => right.priority - left.priority);
    const nextItem = queue.shift();

    if (!nextItem) {
      continue;
    }

    queuedClaimIds.delete(nextItem.claimId);
    currentClaimId = nextItem.claimId;
    notify();

    const result = await analyzeClaim(nextItem.claimId, invalidate);
    processedClaimIds.add(nextItem.claimId);

    if (result.ok) {
      completedCount += 1;
    } else {
      failedCount += 1;
      queueErrors = [...queueErrors, { claimId: nextItem.claimId, message: result.message }];
    }

    currentClaimId = null;
    notify();
  }

  isProcessingQueue = false;
  notify();
}

function enqueueClaimIds(
  claimIds: string[],
  priority: number,
  invalidate: (claimId: string) => Promise<void>,
) {
  let added = false;

  for (const claimId of claimIds) {
    if (
      !claimId ||
      claimId === currentClaimId ||
      queuedClaimIds.has(claimId) ||
      processedClaimIds.has(claimId)
    ) {
      continue;
    }

    queue.push({ claimId, priority });
    queuedClaimIds.add(claimId);
    totalCount += 1;
    added = true;
  }

  if (!added) return;

  notify();
  void processQueue(invalidate);
}

export function useAnalysisQueue(): UseAnalysisQueueReturn {
  const queryClient = useQueryClient();
  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

  const invalidate = async (claimId: string) => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["claims"] }),
      queryClient.invalidateQueries({ queryKey: ["claim-statuses"] }),
      queryClient.invalidateQueries({ queryKey: ["claim-status", claimId] }),
    ]);
  };

  return {
    ...snapshot,
    enqueue: (claimId, priority) => {
      enqueueClaimIds([claimId], priority, invalidate);
    },
    enqueueBatch: (claimIds, priority) => {
      enqueueClaimIds(claimIds, priority, invalidate);
    },
    reset: () => {
      resetQueueState();
    },
  };
}
