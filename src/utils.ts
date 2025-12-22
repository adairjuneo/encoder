/**
 * Formats a number of bytes into a human-readable string with appropriate units (B, KB, MB, GB).
 *
 * @param bytes - The number of bytes to format.
 * @param decimals - The number of decimal places to include in the output.
 * @returns A formatted string representing the byte size.
 */
export function formatBytes(bytes: number, decimals = 2) {
  if (bytes === 0) return '0 Bytes';

  const kilo = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];

  const i = Math.floor(Math.log(bytes) / Math.log(kilo));

  return `${parseFloat((bytes / kilo ** i).toFixed(dm))} ${sizes[i]}`;
}
