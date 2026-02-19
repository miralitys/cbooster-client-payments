interface LoadingSkeletonProps {
  rows?: number;
}

export function LoadingSkeleton({ rows = 5 }: LoadingSkeletonProps) {
  return (
    <div className="cb-skeleton" aria-hidden="true">
      {Array.from({ length: rows }).map((_, index) => (
        <div key={index} className="cb-skeleton__row" />
      ))}
    </div>
  );
}
