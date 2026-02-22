import os
from pathlib import Path

import pandas as pd


TARGET_FEATURES = {
	"PUFNEWEMPSTAT": [
		"PUFNEWEMPSTAT",
		"NEWEMPSTAT",
		"CEMPST1",
		"CEMPST2",
		"NEWEMPST",
	]
}


def read_columns(csv_path: Path) -> list[str]:
	encodings = ["utf-8", "latin-1", "cp1252"]
	last_error = None
	for enc in encodings:
		try:
			df = pd.read_csv(csv_path, nrows=0, encoding=enc, low_memory=False)
			return list(df.columns)
		except Exception as exc:
			last_error = exc
			continue
	raise last_error


def main() -> None:
	raw_dir = Path("./raw")
	files = sorted(
		[p for p in raw_dir.iterdir() if p.is_file() and p.suffix.lower() == ".csv"]
	)

	print(f"Files in directory: {len(files)}")
	if not files:
		return

	totals = {key: 0 for key in TARGET_FEATURES}

	for csv_path in files:
		try:
			columns = read_columns(csv_path)
		except Exception as exc:
			print(f"{csv_path.name}: READ_ERROR ({exc})")
			continue

		col_upper = {c.upper(): c for c in columns}

		for target, sources in TARGET_FEATURES.items():
			matches = [col_upper[s.upper()] for s in sources if s.upper() in col_upper]
			if not matches:
				print(f"{csv_path.name}: {target} -> MISSING")
				continue

			chosen_col = matches[0]
			try:
				df = pd.read_csv(
					csv_path,
					usecols=[chosen_col],
					encoding="latin-1",
					low_memory=False,
				)
			except Exception:
				df = pd.read_csv(
					csv_path,
					usecols=[chosen_col],
					encoding="utf-8",
					errors="ignore",
					low_memory=False,
				)

			total_rows = len(df)
			series = df[chosen_col]
			null_count = int(series.isna().sum())
			value_counts = series.dropna().value_counts().sort_index()
			counts_map = {int(k): int(v) for k, v in value_counts.items() if str(k).isdigit()}

			totals[target] += 1
			count_1 = counts_map.get(1, 0)
			count_2 = counts_map.get(2, 0)
			count_3 = counts_map.get(3, 0)
			print(
				f"{csv_path.name}: {target} -> {chosen_col} | "
				f"1={count_1}, 2={count_2}, 3={count_3}, nulls={null_count}/{total_rows}"
			)

	print("\nSummary")
	for target, count in totals.items():
		print(f"{target}: {count}/{len(files)} files")


if __name__ == "__main__":
	main()