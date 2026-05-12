import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";

const mockFetch = vi.fn();
globalThis.fetch = mockFetch;

let queryClient: QueryClient;

function wrapper({ children }: { children: ReactNode }) {
  if (!queryClient) {
    queryClient = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    });
  }

  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

function createResponse(
  body: Record<string, unknown>,
  status = 200,
): Response {
  return new Response(JSON.stringify(body), { status });
}

describe("useAnalysisQueue", () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockFetch.mockResolvedValue(createResponse({ ok: true }));
    queryClient = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    });
    vi.resetModules();
  });

  afterEach(async () => {
    const { useAnalysisQueue } = await import("@/hooks/use-analysis-queue");
    const { result, unmount } = renderHook(() => useAnalysisQueue(), { wrapper });

    act(() => {
      result.current.reset();
    });

    unmount();
    mockFetch.mockReset();
  });

  it("tracks completion for a bounded batch", async () => {
    const { useAnalysisQueue } = await import("@/hooks/use-analysis-queue");
    const { result } = renderHook(() => useAnalysisQueue(), { wrapper });

    act(() => {
      result.current.enqueueBatch(["C0001", "C0002", "C0003"], 1);
    });

    expect(result.current.progress.total).toBe(3);

    await waitFor(() => {
      expect(result.current.isProcessing).toBe(false);
      expect(result.current.progress.completed).toBe(3);
      expect(result.current.progress.failed).toBe(0);
    });
  });

  it("deduplicates queued and active claim ids", async () => {
    const { useAnalysisQueue } = await import("@/hooks/use-analysis-queue");
    const { result } = renderHook(() => useAnalysisQueue(), { wrapper });

    act(() => {
      result.current.enqueue("C0001", 0);
      result.current.enqueue("C0001", 10);
      result.current.enqueueBatch(["C0001", "C0002"], 1);
    });

    await waitFor(() => {
      expect(result.current.progress.total).toBe(2);
      expect(result.current.progress.completed + result.current.progress.failed).toBe(2);
    });
  });

  it("records failed analyses and keeps deterministic final totals", async () => {
    mockFetch
      .mockResolvedValueOnce(createResponse({ error: "Claim not found" }, 404))
      .mockResolvedValueOnce(createResponse({ ok: true }, 200));

    const { useAnalysisQueue } = await import("@/hooks/use-analysis-queue");
    const { result } = renderHook(() => useAnalysisQueue(), { wrapper });

    act(() => {
      result.current.enqueueBatch(["C0001", "C0002"], 1);
    });

    await waitFor(() => {
      expect(result.current.isProcessing).toBe(false);
      expect(result.current.progress.total).toBe(2);
      expect(result.current.progress.completed).toBe(1);
      expect(result.current.progress.failed).toBe(1);
      expect(result.current.progress.errors).toEqual([
        { claimId: "C0001", message: "Claim not found" },
      ]);
    });
  });

  it("resets queue state explicitly", async () => {
    const { useAnalysisQueue } = await import("@/hooks/use-analysis-queue");
    const { result } = renderHook(() => useAnalysisQueue(), { wrapper });

    act(() => {
      result.current.enqueueBatch(["A", "B"], 1);
    });

    await waitFor(() => {
      expect(result.current.isProcessing).toBe(false);
    });

    act(() => {
      result.current.reset();
    });

    expect(result.current.progress.total).toBe(0);
    expect(result.current.progress.completed).toBe(0);
    expect(result.current.progress.failed).toBe(0);
    expect(result.current.progress.current).toBeNull();
    expect(result.current.progress.errors).toEqual([]);
  });
});
