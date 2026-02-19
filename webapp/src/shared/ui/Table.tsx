import type { ReactNode } from "react";

import { cx } from "@/shared/lib/cx";

export type TableDensity = "compact" | "comfortable";
export type TableAlign = "left" | "center" | "right";

export interface TableColumn<RowType> {
  key: string;
  label: ReactNode;
  align?: TableAlign;
  className?: string;
  cell: (row: RowType, index: number) => ReactNode;
  headerClassName?: string;
}

interface TableProps<RowType> {
  columns: TableColumn<RowType>[];
  rows: RowType[];
  rowKey: (row: RowType, index: number) => string;
  density?: TableDensity;
  emptyState?: ReactNode;
  className?: string;
  tableClassName?: string;
  onRowClick?: (row: RowType, index: number) => void;
  onRowActivate?: (row: RowType, index: number) => void;
  rowClassName?: (row: RowType, index: number) => string | undefined;
  footer?: ReactNode;
}

export function Table<RowType>({
  columns,
  rows,
  rowKey,
  density = "compact",
  emptyState,
  className = "",
  tableClassName = "",
  onRowClick,
  onRowActivate,
  rowClassName,
  footer,
}: TableProps<RowType>) {
  const activationHandler = onRowActivate || onRowClick;

  return (
    <div className={cx("cb-table-wrap", className)}>
      <table className={cx("cb-table", `cb-table--${density}`, tableClassName)}>
        <thead>
          <tr>
            {columns.map((column) => (
              <th
                key={column.key}
                className={cx("cb-table__head-cell", alignmentClass(column.align), column.headerClassName)}
              >
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
                className={cx(
                  "cb-table__row",
                  activationHandler && "cb-table__row--activatable",
                  rowClassName?.(row, index),
                )}
                onClick={
                  activationHandler
                    ? (event) => {
                        if (shouldIgnoreRowActivation(event.target, event.currentTarget)) {
                          return;
                        }
                        activationHandler(row, index);
                      }
                    : undefined
                }
                tabIndex={activationHandler ? 0 : undefined}
                onKeyDown={
                  activationHandler
                    ? (event) => {
                        if (shouldIgnoreRowActivation(event.target, event.currentTarget)) {
                          return;
                        }
                        if (event.key === "Enter" || event.key === " ") {
                          event.preventDefault();
                          activationHandler(row, index);
                        }
                      }
                    : undefined
                }
              >
                {columns.map((column) => (
                  <td
                    key={`${column.key}-${index}`}
                    className={cx("cb-table__cell", alignmentClass(column.align), column.className)}
                  >
                    {column.cell(row, index)}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
        {footer ? <tfoot>{footer}</tfoot> : null}
      </table>
    </div>
  );
}

function alignmentClass(align: TableAlign | undefined): string {
  switch (align) {
    case "right":
      return "cb-table__cell--align-right";
    case "center":
      return "cb-table__cell--align-center";
    default:
      return "cb-table__cell--align-left";
  }
}

function shouldIgnoreRowActivation(target: EventTarget | null, currentTarget: EventTarget | null): boolean {
  if (!(target instanceof Element) || !(currentTarget instanceof HTMLElement)) {
    return false;
  }

  const interactiveRoot = target.closest(
    [
      "button",
      "a",
      "input",
      "select",
      "textarea",
      "[role='button']",
      "[role='link']",
      "[contenteditable='true']",
      "[data-row-activate-ignore='true']",
    ].join(","),
  );

  return Boolean(interactiveRoot && currentTarget.contains(interactiveRoot));
}
