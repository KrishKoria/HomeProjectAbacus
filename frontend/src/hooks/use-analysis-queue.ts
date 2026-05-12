"use client";

import { useCallback, useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";

export interface AnalysisQueueItem {
  claimId: string;
  priority: number;
}

interface UseAnalysisQueueReturn {
  enqueue: (claimId: string, priority: number) => void;
  enqueueBatch: (claimIds: string[], priority: number) => void;
  progress: { total: number; completed: number; current: string | null };
  isProcessing: boolean;
}

const gQueue: AnalysisQueueItem[] = [];
let gProcessing = false;
let gPaused = false;
let gCurrent: string | null = null;
let gTotal = 0;
let gCompleted = 0;
const gListeners = new Set<() => void>();

async function analyzeClaim(claimId: string, queryClient: ReturnType<typeof useQueryClient>) {
  try {
    await fetch("/api/claims/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ claimId }),
    });
  } catch {
    /* individual claim failures are non-fatal */
  }
  gCompleted++;
  gCurrent = null;
  queryClient.invalidateQueries({ queryKey: ["claims"] });
}

async function processQueue(queryClient: ReturnType<typeof useQueryClient>) {
  if (gProcessing) return;
  gProcessing = true;

  while (gQueue.length > 0) {
    if (gPaused) {
      await new Promise((r) => setTimeout(r, 200));
      continue;
    }

    gQueue.sort((a, b) => b.priority - a.priority);
    const item = gQueue.shift()!;
    gCurrent = item.claimId;
    gListeners.forEach((fn) => fn());

    await analyzeClaim(item.claimId, queryClient);
    gListeners.forEach((fn) => fn());
  }

  gProcessing = false;
  gListeners.forEach((fn) => fn());
}

export function useAnalysisQueue(): UseAnalysisQueueReturn {
  const queryClient = useQueryClient();

  const [progress, setProgress] = useState({
    total: gTotal,
    completed: gCompleted,
    current: gCurrent,
  });

  const [isProcessing, setIsProcessing] = useState(gProcessing);

  useEffect(() => {
    const listener = () => {
      setProgress({ total: gTotal, completed: gCompleted, current: gCurrent });
      setIsProcessing(gProcessing);
    };
    gListeners.add(listener);
    return () => {
      gListeners.delete(listener);
    };
  }, []);

  const enqueue = useCallback(
    (claimId: string, priority: number) => {
      const exists = gQueue.some((item) => item.claimId === claimId) || gCurrent === claimId;
      if (exists) return;
      gQueue.push({ claimId, priority });
      gTotal++;
      gListeners.forEach((fn) => fn());
      processQueue(queryClient);
    },
    [queryClient],
  );

  const enqueueBatch = useCallback(
    (claimIds: string[], priority: number) => {
      for (const claimId of claimIds) {
        const exists = gQueue.some((item) => item.claimId === claimId) || gCurrent === claimId;
        if (exists) continue;
        gQueue.push({ claimId, priority });
        gTotal++;
      }
      gListeners.forEach((fn) => fn());
      processQueue(queryClient);
    },
    [queryClient],
  );

  return { enqueue, enqueueBatch, progress, isProcessing };
}

export function pauseAnalysisQueue() {
  gPaused = true;
  gListeners.forEach((fn) => fn());
}

export function resumeAnalysisQueue() {
  gPaused = false;
  gListeners.forEach((fn) => fn());
}
