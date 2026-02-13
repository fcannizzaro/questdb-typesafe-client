import Link from "next/link";
import Image from "next/image";

export default function HomePage() {
  return (
    <div className="flex flex-col justify-center text-center flex-1 gap-4">
      <Image src="icon.png" width={240} height={240} objectFit="" className="mx-auto" alt="icon" />
      <h1 className="text-4xl font-bold">questdb-typesafe-client</h1>
      <p className="text-lg text-fd-muted-foreground max-w-lg mx-auto">
        Type-safe QuestDB client for TypeScript — schema definitions, query
        builders, and DDL with full type inference.
      </p>
      <div className="flex gap-3 justify-center mt-2">
        <Link
          href="/docs"
          className="px-4 py-2 rounded-md bg-fd-primary text-fd-primary-foreground font-medium text-sm hover:opacity-90 transition-opacity"
        >
          Get Started
        </Link>
        <Link
          href="https://github.com/fcannizzaro/questdb-typesafe-client"
          className="px-4 py-2 rounded-md border border-fd-border font-medium text-sm hover:bg-fd-accent transition-colors"
        >
          GitHub
        </Link>
      </div>
    </div>
  );
}
