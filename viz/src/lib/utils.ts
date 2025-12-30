import { type ClassValue, clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs: ClassValue[]) {
	return twMerge(clsx(inputs));
}

/**
 * Formats a number in a human-readable format (billion, million, thousand)
 * @param value - The number to format
 * @returns Formatted string like "$1.5B", "$50M", "$1.5K", etc.
 */
export function formatCurrency(value: number): string {
	const absValue = Math.abs(value);
	const sign = value < 0 ? '-' : '';

	if (absValue >= 1_000_000_000) {
		return `${sign}$${(absValue / 1_000_000_000).toFixed(1)}B`;
	} else if (absValue >= 1_000_000) {
		return `${sign}$${(absValue / 1_000_000).toFixed(1)}M`;
	} else if (absValue >= 1_000) {
		return `${sign}$${(absValue / 1_000).toFixed(1)}K`;
	} else {
		return `${sign}$${absValue.toFixed(0)}`;
	}
}
