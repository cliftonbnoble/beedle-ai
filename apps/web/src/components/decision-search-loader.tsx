"use client";

export function DecisionSearchLoader() {
  return (
    <section
      className="card"
      style={{
        padding: "0.95rem 1rem",
        marginTop: "1rem",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        gap: "0.95rem",
        background: "rgba(20, 93, 160, 0.035)",
        borderColor: "rgba(20, 93, 160, 0.12)"
      }}
    >
      <div style={{ width: 94, height: 94, flex: "0 0 auto", overflow: "visible", marginTop: "0.15rem" }} aria-hidden="true">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="30 102 280 186" width="100%" height="100%" style={{ overflow: "visible" }}>
          <style>
            {`
              .gavel-loader-gavel {
                transform-origin: 270px 182px;
                animation: gavel-strike 2.5s infinite;
              }

              .gavel-loader-wave {
                transform-origin: 120px 240px;
                animation: gavel-wave 2.5s infinite;
              }

              @keyframes gavel-strike {
                0% {
                  transform: rotate(55deg);
                  animation-timing-function: cubic-bezier(0.5, 0, 1, 1);
                }
                12% {
                  transform: rotate(0deg);
                  animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
                }
                22% {
                  transform: rotate(25deg);
                  animation-timing-function: cubic-bezier(0.5, 0, 1, 1);
                }
                32% {
                  transform: rotate(0deg);
                  animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
                }
                45% {
                  transform: rotate(55deg);
                  animation-timing-function: cubic-bezier(0.4, 0, 0.2, 1);
                }
                100% {
                  transform: rotate(55deg);
                }
              }

              @keyframes gavel-wave {
                0%, 11%, 21%, 31%, 40%, 100% {
                  opacity: 0;
                  transform: scale(0.65);
                }
                12%, 32% {
                  opacity: 0.72;
                  transform: scale(0.94);
                  stroke-width: 4px;
                }
                18%, 38% {
                  opacity: 0;
                  transform: scale(1.28);
                  stroke-width: 1px;
                }
              }
            `}
          </style>

          <g id="sounding-block">
            <path d="M 35 270 L 35 278 A 85 22 0 0 0 205 278 L 205 270 Z" fill="#20110A" />
            <ellipse cx="120" cy="270" rx="85" ry="22" fill="#3B2215" />

            <path d="M 45 255 L 45 263 A 75 18 0 0 0 195 263 L 195 255 Z" fill="#2E1A0F" />
            <ellipse cx="120" cy="255" rx="75" ry="18" fill="#4D2E1C" />

            <path d="M 55 240 L 55 248 A 65 15 0 0 0 185 248 L 185 240 Z" fill="#3A2214" />
            <ellipse cx="120" cy="240" rx="65" ry="15" fill="#5E3823" />

            <ellipse cx="120" cy="240" rx="52" ry="11" fill="none" stroke="#462A19" strokeWidth="2" />
            <ellipse cx="120" cy="240" rx="40" ry="8" fill="none" stroke="#462A19" strokeWidth="1" />
          </g>

          <ellipse className="gavel-loader-wave" cx="120" cy="240" rx="65" ry="15" fill="none" stroke="#D99A2B" strokeLinecap="round" />

          <g className="gavel-loader-gavel">
            <g transform="translate(270, 190)">
              <path d="M -118,-8 C -70,-8 -30,-20 0,-20 C 16,-20 16,20 0,20 C -30,20 -70,8 -118,8 Z" fill="#311C10" />
              <path d="M -118,-8 C -70,-8 -30,-18 0,-18 C 13,-18 13,18 0,18 C -30,18 -70,8 -118,8 Z" fill="#5E3823" />
              <path d="M -112,-5 C -70,-5 -30,-14 0,-14" stroke="#7A4C31" strokeWidth="4" strokeLinecap="round" fill="none" />

              <rect x="-126" y="-13" width="10" height="26" rx="3" fill="#311C10" />
              <rect x="-124" y="-12" width="6" height="24" rx="2" fill="#5E3823" />
              <rect x="-124" y="-12" width="6" height="4" fill="#7A4C31" />

              <g>
                <rect x="-175" y="-58" width="60" height="22" rx="11" fill="#311C10" />
                <rect x="-173" y="-58" width="56" height="22" rx="11" fill="#5E3823" />
                <rect x="-168" y="-54" width="6" height="14" rx="3" fill="#7A4C31" />

                <rect x="-163" y="-36" width="36" height="12" fill="#2E1A0F" />
                <rect x="-158" y="-36" width="4" height="12" fill="#4D2E1C" />

                <rect x="-168" y="-24" width="46" height="48" rx="4" fill="#311C10" />
                <rect x="-166" y="-24" width="44" height="48" rx="4" fill="#5E3823" />
                <rect x="-162" y="-22" width="6" height="44" rx="2" fill="#7A4C31" />

                <rect x="-171" y="-12" width="52" height="24" rx="2" fill="#C79A3A" />
                <rect x="-164" y="-12" width="8" height="24" fill="#F6D365" />
                <rect x="-135" y="-12" width="14" height="24" fill="#8C6621" />
                <rect x="-171" y="-12" width="52" height="2" fill="#FFF" opacity="0.4" />
                <rect x="-171" y="10" width="52" height="2" fill="#5A3F0E" opacity="0.6" />

                <rect x="-163" y="24" width="36" height="12" fill="#2E1A0F" />
                <rect x="-158" y="24" width="4" height="12" fill="#4D2E1C" />

                <rect x="-175" y="36" width="60" height="22" rx="11" fill="#311C10" />
                <rect x="-173" y="36" width="56" height="22" rx="11" fill="#5E3823" />
                <rect x="-168" y="40" width="6" height="14" rx="3" fill="#7A4C31" />
              </g>
            </g>
          </g>
        </svg>
      </div>

      <div style={{ display: "grid", gap: "0.22rem", minWidth: 0 }}>
        <strong style={{ fontSize: "1rem", lineHeight: 1.2 }}>Searching decisions...</strong>
        <p style={{ margin: 0, color: "var(--muted)", lineHeight: 1.45, fontSize: "0.9rem" }}>
          Reviewing the corpus and ranking the strongest matches.
        </p>
      </div>
    </section>
  );
}
