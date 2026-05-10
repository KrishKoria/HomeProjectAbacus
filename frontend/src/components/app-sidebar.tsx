"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarHeader,
  SidebarFooter,
} from "@/components/ui/sidebar";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { SignOut, ChartBar, MagnifyingGlass } from "@phosphor-icons/react";

const navItems = [
  { label: "Dashboard", href: "/dashboard", icon: ChartBar },
  { label: "Search Claims", href: "/dashboard", icon: MagnifyingGlass },
];

export function AppSidebar() {
  const pathname = usePathname();
  const router = useRouter();

  return (
    <Sidebar>
      <SidebarHeader className="px-4 py-3">
        <Link href="/dashboard" className="text-sm font-medium tracking-tight">
          ClaimOps
        </Link>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Navigation</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {navItems.map((item) => {
                const Icon = item.icon;
                return (
                  <SidebarMenuItem key={item.label}>
                    <SidebarMenuButton
                      isActive={pathname === item.href}
                      render={<Link href={item.href} />}
                    >
                      <Icon />
                      <span>{item.label}</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter className="p-4">
        <Button
          variant="outline"
          size="sm"
          className="w-full"
          onClick={() => {
            fetch("/api/auth/sign-out", { method: "POST" }).then(() => {
              router.push("/sign-in");
            });
          }}
        >
          <SignOut />
          Sign Out
        </Button>
      </SidebarFooter>
    </Sidebar>
  );
}
