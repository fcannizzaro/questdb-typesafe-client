/**
 * Escape a QuestDB identifier (table name, column name).
 * QuestDB uses double quotes for identifier escaping.
 */
export function escapeIdentifier(name: string): string {
  if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    return name;
  }
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Escape a string literal value.
 * QuestDB uses single quotes, with '' for escaping.
 */
export function escapeString(value: string): string {
  return value.replace(/'/g, "''");
}
