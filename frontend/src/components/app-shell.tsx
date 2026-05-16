"use client";

import { useQuery } from "@tanstack/react-query";
import { Circle } from "@phosphor-icons/react";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/app-sidebar";
import { SidebarInset } from "@/components/ui/sidebar";
import { SkipToContent } from "@/components/skip-to-content";
import {
  Tooltip,
  TooltipTrigger,
  TooltipContent,
  TooltipProvider,
} from "@/components/ui/tooltip";
import type { DatabricksStatus } from "@/lib/databricks/types";

interface AppShellProps {
  children: React.ReactNode;
  breadcrumb?: React.ReactNode;
}

function StatusDot({ name, ok }: { name: string; ok: boolean }) {
  const label = `${name}: ${ok ? "connected" : "offline"}`;
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            role="img"
            aria-label={label}
            className="flex items-center justify-center size-5 cursor-default"
          >
            <Circle
              size={8}
              weight="fill"
              className={ok ? "text-status-ok" : "text-status-err"}
              aria-hidden="true"
            />
          </span>
        }
      />
      <TooltipContent side="bottom">
        <span>{label}</span>
      </TooltipContent>
    </Tooltip>
  );
}

export function AppShell({ children, breadcrumb }: AppShellProps) {
  const statusQuery = useQuery({
    queryKey: ["runtime-status"],
    queryFn: async () => {
      const res = await fetch("/api/runtime/status");
      if (!res.ok) throw new Error("Failed to fetch status");
      return res.json() as Promise<DatabricksStatus>;
    },
    refetchInterval: 30_000,
  });

  return (
    <>
      <SkipToContent />
      <AppSidebar />
      <SidebarInset>
        <header className="flex h-12 items-center gap-3 px-4 border-b border-border shrink-0">
          <SidebarTrigger className="p-1.5 size-4 text-muted-foreground hover:text-foreground transition-colors" />
          {breadcrumb && (
            <span className="type-caption text-muted-foreground">{breadcrumb}</span>
          )}
          {statusQuery.data && (
            <TooltipProvider>
              <div
                className="ml-auto flex items-center gap-2"
                aria-label="System status"
              >
                <StatusDot name="OAuth" ok={statusQuery.data.oauth} />
                <StatusDot name="SQL warehouse" ok={statusQuery.data.sqlWarehouse} />
                <StatusDot name="Model" ok={statusQuery.data.analysisEndpoint} />
              </div>
            </TooltipProvider>
          )}
        </header>
        <div id="main-content" role="main" className="flex-1 min-h-0">
          {children}
        </div>
      </SidebarInset>
    </>
  );
}
