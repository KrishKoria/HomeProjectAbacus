import { pgTable, text, timestamp, boolean, real, integer } from "drizzle-orm/pg-core";

export const user = pgTable("user", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  email: text("email").notNull().unique(),
  emailVerified: boolean("email_verified").notNull().default(false),
  image: text("image"),
  createdAt: timestamp("created_at").notNull(),
  updatedAt: timestamp("updated_at").notNull(),
  role: text("role").notNull().default("user"),
  status: text("status").notNull().default("active"),
});

export const session = pgTable("session", {
  id: text("id").primaryKey(),
  userId: text("user_id")
    .notNull()
    .references(() => user.id, { onDelete: "cascade" }),
  token: text("token").notNull().unique(),
  expiresAt: timestamp("expires_at").notNull(),
  ipAddress: text("ip_address"),
  userAgent: text("user_agent"),
  createdAt: timestamp("created_at").notNull(),
  updatedAt: timestamp("updated_at").notNull(),
});

export const account = pgTable("account", {
  id: text("id").primaryKey(),
  userId: text("user_id")
    .notNull()
    .references(() => user.id, { onDelete: "cascade" }),
  accountId: text("account_id").notNull(),
  providerId: text("provider_id").notNull(),
  accessToken: text("access_token"),
  refreshToken: text("refresh_token"),
  accessTokenExpiresAt: timestamp("access_token_expires_at"),
  refreshTokenExpiresAt: timestamp("refresh_token_expires_at"),
  scope: text("scope"),
  idToken: text("id_token"),
  password: text("password"),
  createdAt: timestamp("created_at").notNull(),
  updatedAt: timestamp("updated_at").notNull(),
});

export const verification = pgTable("verification", {
  id: text("id").primaryKey(),
  identifier: text("identifier").notNull(),
  value: text("value").notNull(),
  expiresAt: timestamp("expires_at").notNull(),
  createdAt: timestamp("created_at").notNull(),
  updatedAt: timestamp("updated_at").notNull(),
});

export const claimReviews = pgTable("claim_reviews", {
  id: text("id").primaryKey(),
  claimId: text("claim_id").notNull().unique(),
  riskScore: real("risk_score"),
  riskLevel: text("risk_level"),
  narrative: text("narrative").default(""),
  topReason: text("top_reason"),
  status: text("status").notNull().default("new"),
  analyzedAt: timestamp("analyzed_at"),
  reviewedAt: timestamp("reviewed_at"),
  reviewedById: text("reviewed_by_id").references(() => user.id),
});

export const claimSyncState = pgTable("claim_sync_state", {
  sourceTable: text("source_table").primaryKey(),
  lastIngestedAt: timestamp("last_ingested_at"),
  lastClaimId: text("last_claim_id"),
  lastSyncedAt: timestamp("last_synced_at").notNull(),
  lastDiscoveredCount: integer("last_discovered_count").notNull().default(0),
  lastInsertedCount: integer("last_inserted_count").notNull().default(0),
});
