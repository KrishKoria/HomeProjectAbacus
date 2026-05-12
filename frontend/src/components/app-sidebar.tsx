"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarHeader,
  SidebarFooter,
} from "@/components/ui/sidebar";
import { useQuery } from "@tanstack/react-query";
import { ClipboardText, ChartBar, SignOut } from "@phosphor-icons/react";

const navItems = [
  { label: "Claims", href: "/claims", icon: ClipboardText, shortcut: "G C" },
  { label: "Dashboard", href: "/dashboard", icon: ChartBar, shortcut: "G D" },
];

export function AppSidebar() {
  const pathname = usePathname();

  const sessionQuery = useQuery({
    queryKey: ["session"],
    queryFn: async () => {
      const res = await fetch("/api/me");
      if (!res.ok) return null;
      return res.json() as Promise<{ user: { name: string; email: string } }>;
    },
    staleTime: 60_000,
  });

  const user = sessionQuery.data?.user;

  return (
    <Sidebar>
      <SidebarHeader className="px-4 py-3 border-b border-sidebar-border">
        <Link
          href="/claims"
          className="text-sm font-semibold tracking-tight text-sidebar-foreground hover:text-sidebar-primary transition-colors"
        >
          ClaimOps
        </Link>
      </SidebarHeader>

      <SidebarContent className="pt-2">
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {navItems.map((item) => {
                const Icon = item.icon;
                const isActive =
                  item.href === "/claims"
                    ? pathname === "/claims" || pathname.startsWith("/claims/")
                    : pathname === item.href;

                return (
                  <SidebarMenuItem key={item.label}>
                    <SidebarMenuButton
                      isActive={isActive}
                      render={<Link href={item.href} />}
                    >
                      <Icon weight={isActive ? "fill" : "regular"} />
                      <span className="text-sm font-medium">{item.label}</span>
                      {item.shortcut && (
                        <span className="ml-auto text-xs text-muted-foreground font-mono opacity-60">
                          {item.shortcut}
                        </span>
                      )}
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter className="p-4 border-t border-sidebar-border space-y-3">
        {user && (
          <div className="px-1 space-y-0.5">
            <p className="text-xs font-medium text-sidebar-foreground truncate">{user.name}</p>
            <p className="type-caption text-muted-foreground truncate">{user.email}</p>
          </div>
        )}
        <SidebarMenuButton
          render={<button />}
          onClick={() => {
            fetch("/api/auth/sign-out", { method: "POST" }).then(() => {
              window.location.href = "/sign-in";
            });
          }}
          className="w-full text-muted-foreground hover:text-foreground"
        >
          <SignOut />
          <span className="text-sm">Sign out</span>
        </SidebarMenuButton>
      </SidebarFooter>
    </Sidebar>
  );
}
