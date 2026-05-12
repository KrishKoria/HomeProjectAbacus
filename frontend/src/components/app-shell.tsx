"use client";

import { SidebarTrigger } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/app-sidebar";
import { SidebarInset } from "@/components/ui/sidebar";

interface AppShellProps {
  children: React.ReactNode;
  breadcrumb?: React.ReactNode;
}

export function AppShell({ children, breadcrumb }: AppShellProps) {
  return (
    <>
      <AppSidebar />
      <SidebarInset>
        <header className="flex h-12 items-center gap-3 px-4 border-b border-border shrink-0">
          <SidebarTrigger className="size-4 text-muted-foreground hover:text-foreground transition-colors" />
          {breadcrumb && (
            <span className="type-caption text-muted-foreground">{breadcrumb}</span>
          )}
        </header>
        <main id="main-content" className="flex-1 min-h-0">
          {children}
        </main>
      </SidebarInset>
    </>
  );
}
