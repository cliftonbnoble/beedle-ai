import fs from "node:fs/promises";
import path from "node:path";

const reportsDir = path.resolve(process.cwd(), "reports");
const worksheetJsonPath = path.resolve(reportsDir, "provisional-topic-intake-worksheet.json");
const worksheetCsvPath = path.resolve(reportsDir, "provisional-topic-intake-worksheet.csv");
const outputJsonPath = path.resolve(reportsDir, "provisional-topic-intake-worksheet-prefilled.json");
const outputCsvPath = path.resolve(reportsDir, "provisional-topic-intake-worksheet-prefilled.csv");
const outputMdPath = path.resolve(reportsDir, "provisional-topic-intake-worksheet-prefilled.md");
const baselineJsonPath = path.resolve(reportsDir, "retrieval-health-before-next-import.json");
const baselineMdPath = path.resolve(reportsDir, "retrieval-health-before-next-import.md");
const healthJsonPath = path.resolve(reportsDir, "retrieval-health-report.json");
const healthMdPath = path.resolve(reportsDir, "retrieval-health-report.md");

const SEED_CANDIDATES = {
  cooling: [
    {
      candidateSourceUrl: "https://sfrb.org/sites/default/files/minutes%20041222.pdf",
      candidateTitle: "1770 Green Street #104 - excessive heat in unit",
      candidateCitation: "AT220006",
      sourceType: "official_minutes_pdf_reference",
      notes: "Official Rent Board minutes reference a tenant petition alleging decreased housing services due to excessive heat; candidate for underlying decision acquisition."
    },
    {
      candidateSourceUrl: "https://sfrb.org/sites/default/files/Document/Minutes/minutes%20041321.pdf",
      candidateTitle: "785 Valencia Street - inadequate heat and bathroom mold",
      candidateCitation: "AL210013",
      sourceType: "official_minutes_pdf_reference",
      notes: "Official Rent Board minutes reference inadequate heat, bathroom mold, and maintenance issues; useful thermal-condition candidate."
    },
    {
      candidateSourceUrl: "https://sfrb.org/sites/default/files/Document/Minutes/minutes%20021621.pdf",
      candidateTitle: "748 Page Street #9 - mold and inadequate ventilation",
      candidateCitation: "AL210005",
      sourceType: "official_minutes_pdf_reference",
      notes: "Official Rent Board minutes reference mold and inadequate ventilation in bathroom; strong crossover candidate for cooling/ventilation."
    },
    {
      candidateSourceUrl: "https://sfrb.org/ftp/meetingarchive/index_894_e291.html?page=894",
      candidateTitle: "3220 23rd Street #A - loss of air quality and ventilation",
      candidateCitation: "AT170114",
      sourceType: "official_minutes_html_reference",
      notes: "Archived minutes reference loss of air quality and ventilation as part of decreased housing services appeal."
    }
  ],
  ventilation: [
    {
      candidateSourceUrl: "https://sfrb.org/sites/default/files/Document/Minutes/minutes%20021621.pdf",
      candidateTitle: "748 Page Street #9 - mold and inadequate ventilation",
      candidateCitation: "AL210005",
      sourceType: "official_minutes_pdf_reference",
      notes: "Official Rent Board minutes reference loss of use of bathroom due to mold and inadequate ventilation."
    },
    {
      candidateSourceUrl: "https://sfrb.org/ftp/meetingarchive/index_894_e291.html?page=894",
      candidateTitle: "3220 23rd Street #A - loss of air quality and ventilation",
      candidateCitation: "AT170114",
      sourceType: "official_minutes_html_reference",
      notes: "Archived minutes reference loss of air quality and ventilation not fully considered on appeal."
    },
    {
      candidateSourceUrl: "https://sfrb.org/sites/default/files/Document/Minutes/minutes%20031621.pdf",
      candidateTitle: "900 Chestnut Street #310 - bathroom mold incidents",
      candidateCitation: "AT210008",
      sourceType: "official_minutes_pdf_reference",
      notes: "Official Rent Board minutes reference mold incidents and bathroom wall condition; likely to include ventilation-related factual context."
    },
    {
      candidateSourceUrl: "https://sfrb.org/sites/default/files/Document/Minutes/minutes%20041321.pdf",
      candidateTitle: "785 Valencia Street - bathroom mold and upkeep conditions",
      candidateCitation: "AL210013",
      sourceType: "official_minutes_pdf_reference",
      notes: "Official Rent Board minutes reference bathroom mold and general upkeep claims; useful ventilation-adjacent candidate."
    }
  ],
  mold: [
    {
      candidateSourceUrl: "https://sfrb.org/sites/default/files/Document/Minutes/minutes%20021621.pdf",
      candidateTitle: "748 Page Street #9 - mold and inadequate ventilation",
      candidateCitation: "AL210005",
      sourceType: "official_minutes_pdf_reference",
      notes: "Strong mold-specific candidate from official Rent Board minutes."
    },
    {
      candidateSourceUrl: "https://sfrb.org/sites/default/files/Document/Minutes/minutes%20031621.pdf",
      candidateTitle: "900 Chestnut Street #310 - mold remediation and mold allergies",
      candidateCitation: "AT210008",
      sourceType: "official_minutes_pdf_reference",
      notes: "Official Rent Board minutes reference multiple mold incidents and tenant mold allergies."
    },
    {
      candidateSourceUrl: "https://sfrb.org/ftp/meetingarchive/Modules/m101910__cbfb.pdf?documentid=2430",
      candidateTitle: "425 Morse Street Upper - leaking roof with ceiling damage and mold",
      candidateCitation: "AL100091",
      sourceType: "official_minutes_pdf_reference",
      notes: "Archived official minutes reference ceiling damage and mold with granted decreased housing services claim."
    },
    {
      candidateSourceUrl: "https://sfrb.org/sites/default/files/Document/Minutes/minutes%20011618.pdf",
      candidateTitle: "922 Post Street #507 - lead paint dust, mold, and paint conditions",
      candidateCitation: "AT170112",
      sourceType: "official_minutes_pdf_reference",
      notes: "Archived official minutes reference mold and paint-condition problems tied to decreased housing services appeal."
    }
  ]
};

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function toCsv(rows) {
  const columns = [
    "topic",
    "priority",
    "currentZeroResults",
    "targetNewDecisions",
    "acquisitionRank",
    "searchPrompt",
    "expectedTerms",
    "candidateSourceUrl",
    "candidateTitle",
    "candidateCitation",
    "sourceType",
    "importStatus",
    "notes",
    "keepIfFound"
  ];
  return [
    columns.join(","),
    ...rows.map((row) => columns.map((column) => csvEscape(row[column])).join(","))
  ].join("\n");
}

function toMarkdown(report) {
  const lines = [
    "# Provisional Topic Intake Worksheet Prefilled",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Baseline retrieval health snapshot: \`${path.basename(baselineMdPath)}\``,
    `- Row count: \`${report.summary.rowCount}\``,
    `- Topics covered: \`${report.summary.topicCount}\``,
    "",
    "## Prefill Notes",
    "",
    "- These are official Rent Board minutes references for likely underlying decisions to acquire.",
    "- They are intake candidates, not yet imported decision documents.",
    "- Keep the gated reprocess path unchanged while pursuing these source decisions.",
    ""
  ];

  for (const topic of ["cooling", "ventilation", "mold"]) {
    lines.push(`## ${topic}`);
    lines.push("");
    for (const row of report.rows.filter((entry) => entry.topic === topic)) {
      lines.push(
        `- rank=${row.acquisitionRank} | \`${row.candidateCitation}\` | ${row.candidateTitle} | [source](${row.candidateSourceUrl})`
      );
    }
    lines.push("");
  }

  lines.push("## Batch Use");
  lines.push("");
  lines.push("1. Treat the current retrieval health report copy as the before snapshot.");
  lines.push("2. Acquire the underlying decision sources behind these references.");
  lines.push("3. Import in a small batch.");
  lines.push("4. Rerun `pnpm --dir '/Users/cliftonnoble/Documents/Beedle AI App/apps/api' report:retrieval-health`.");
  lines.push("5. Compare the new health report against the saved baseline.");
  lines.push("");

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const worksheet = await readJson(worksheetJsonPath);
  const rows = (worksheet.rows || []).map((row) => {
    const topicRows = SEED_CANDIDATES[row.topic] || [];
    const candidate = topicRows[row.acquisitionRank - 1] || null;
    if (!candidate) {
      return row;
    }
    return {
      ...row,
      ...candidate,
      importStatus: "candidate_identified",
      keepIfFound: "yes"
    };
  });

  const report = {
    generatedAt: new Date().toISOString(),
    sourceWorksheetPath: worksheetJsonPath,
    summary: {
      rowCount: rows.length,
      topicCount: new Set(rows.map((row) => row.topic)).size
    },
    rows
  };

  await Promise.all([
    fs.writeFile(outputJsonPath, JSON.stringify(report, null, 2)),
    fs.writeFile(outputCsvPath, toCsv(rows)),
    fs.writeFile(outputMdPath, toMarkdown(report)),
    fs.copyFile(healthJsonPath, baselineJsonPath),
    fs.copyFile(healthMdPath, baselineMdPath)
  ]);

  console.log(JSON.stringify({ ...report.summary, baselineSaved: true }, null, 2));
  console.log(`Prefilled intake worksheet JSON written to ${outputJsonPath}`);
  console.log(`Prefilled intake worksheet CSV written to ${outputCsvPath}`);
  console.log(`Prefilled intake worksheet Markdown written to ${outputMdPath}`);
  console.log(`Baseline retrieval health JSON copied to ${baselineJsonPath}`);
  console.log(`Baseline retrieval health Markdown copied to ${baselineMdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
