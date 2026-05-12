import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, act, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";

const mockFetch = vi.fn();
globalThis.fetch = mockFetch;

let queryClient: QueryClient;

function wrapper({ children }: { children: ReactNode }) {
  if (!queryClient) {
    queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
  }
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

function createMockResponse(): Response {
  return new Response(JSON.stringify({ ok: true }), { status: 200 });
}

describe("useAnalysisQueue", () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockFetch.mockResolvedValue(createMockResponse());
    queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    vi.resetModules();
  });

  afterEach(() => {
    mockFetch.mockReset();
  });

  it("should enqueue items and report progress", async () => {
    const { useAnalysisQueue } = await import("@/hooks/use-analysis-queue");

    const { result } = renderHook(() => useAnalysisQueue(), { wrapper });

    expect(result.current.progress.total).toBe(0);
    expect(result.current.progress.completed).toBe(0);

    act(() => {
      result.current.enqueueBatch(["C0001", "C0002", "C0003"], 0);
    });

    expect(result.current.progress.total).toBe(3);

    await waitFor(
      () => {
        expect(result.current.progress.completed).toBe(3);
      },
      { timeout: 5000 },
    );
  });

  it("should deduplicate claim IDs", async () => {
    const { useAnalysisQueue } = await import("@/hooks/use-analysis-queue");

    const { result } = renderHook(() => useAnalysisQueue(), { wrapper });

    act(() => {
      result.current.enqueue("C0001", 0);
      result.current.enqueue("C0001", 10);
    });

    await waitFor(() => {
      expect(result.current.progress.total).toBe(1);
    });
  });

  it("should batch enqueue with correct count", async () => {
    const { useAnalysisQueue } = await import("@/hooks/use-analysis-queue");

    const { result } = renderHook(() => useAnalysisQueue(), { wrapper });

    act(() => {
      result.current.enqueueBatch(["A", "B", "C", "D", "E"], 1);
    });

    expect(result.current.progress.total).toBe(5);

    await waitFor(
      () => {
        expect(result.current.progress.completed).toBe(5);
      },
      { timeout: 5000 },
    );
  });
});
