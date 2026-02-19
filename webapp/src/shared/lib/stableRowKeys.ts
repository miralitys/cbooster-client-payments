import type { MutableRefObject } from "react";

export type RowWithKey<RowType> = RowType & {
  _rowKey: string;
};

export function withStableRowKeys<RowType extends object>(
  nextRows: RowType[],
  previousRows: Array<RowWithKey<RowType>>,
  sequenceRef: MutableRefObject<number>,
  options: {
    prefix: string;
    signature: (row: RowType) => string;
  },
): Array<RowWithKey<RowType>> {
  const reusableKeysBySignature = new Map<string, string[]>();

  for (const row of previousRows) {
    const signature = options.signature(row);
    const existing = reusableKeysBySignature.get(signature);
    if (existing) {
      existing.push(row._rowKey);
    } else {
      reusableKeysBySignature.set(signature, [row._rowKey]);
    }
  }

  return nextRows.map((row) => {
    const signature = options.signature(row);
    const reusable = reusableKeysBySignature.get(signature);
    const rowKey = reusable && reusable.length ? reusable.shift() || `${options.prefix}-${++sequenceRef.current}` : `${options.prefix}-${++sequenceRef.current}`;
    return {
      ...row,
      _rowKey: rowKey,
    };
  });
}
