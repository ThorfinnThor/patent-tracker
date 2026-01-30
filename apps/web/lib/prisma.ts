import { PrismaClient } from "@prisma/client";

const globalAny = global as any;

export const prisma: PrismaClient =
  globalAny.__prisma ??
  new PrismaClient({
    log: ["error"],
  });

if (process.env.NODE_ENV !== "production") globalAny.__prisma = prisma;
