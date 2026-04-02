type StatusPillProps = {
  label: string;
  stateLabel?: string;
};

export function StatusPill({ label, stateLabel = "ONLINE" }: StatusPillProps) {
  return (
    <div className="status-pill">
      <div className="status-pill__main">
        <span className="status-pill__indicator" aria-hidden="true">
          <span className="status-pill__dot" />
        </span>
        <span className="status-pill__label">{label}</span>
      </div>
      <span className="status-pill__state">{stateLabel}</span>
    </div>
  );
}
