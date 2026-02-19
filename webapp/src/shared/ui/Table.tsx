import type { ReactNode } from "react";

export interface TableColumn<RowType> {
  key: string;
  label: string;
  className?: string;
  cell: (row: RowType) => ReactNode;
  headerClassName?: string;
}

interface TableProps<RowType> {
  columns: TableColumn<RowType>[];
  rows: RowType[];
  rowKey: (row: RowType, index: number) => string;
  emptyState?: ReactNode;
  className?: string;
  onRowClick?: (row: RowType) => void;
}

export function Table<RowType>({
  columns,
  rows,
  rowKey,
  emptyState,
  className = "",
  onRowClick,
}: TableProps<RowType>) {
  return (
    <div className={`cb-table-wrap ${className}`.trim()}>
      <table className="cb-table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column.key} className={column.headerClassName || ""}>
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {!rows.length ? (
            <tr>
              <td colSpan={columns.length} className="cb-table__empty">
                {emptyState || "No records"}
              </td>
            </tr>
          ) : (
            rows.map((row, index) => (
              <tr
                key={rowKey(row, index)}
                className={onRowClick ? "is-clickable" : ""}
                onClick={onRowClick ? () => onRowClick(row) : undefined}
              >
                {columns.map((column) => (
                  <td key={`${column.key}-${index}`} className={column.className || ""}>
                    {column.cell(row)}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
