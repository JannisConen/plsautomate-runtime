# Test Fixtures — SAP Order Processing

Simulates the scenario: incoming customer orders arrive as a ZIP of folders (one folder per order number), each containing the associated documents. Order metadata and line items come from SAP VBAK/VBAP exports.

## Files

| File | Type | Description |
|------|------|-------------|
| `orders.zip` | ZIP | 4 order folders, each named by order number (VBELN), containing 1–3 PDFs |
| `VBAK.xlsx` | XLSX | SAP order headers (VBELN, ERDAT, KUNNR, NAME1, NETWR, …) |
| `VBAP.xlsx` | XLSX | SAP order line items (VBELN, POSNR, MATNR, ARKTX, KWMENG, …) |

### orders.zip structure

```
orders.zip
├── 0000012345/
│   ├── Angebot_12345.pdf
│   └── Spezifikation_12345.pdf
├── 0000012346/
│   ├── Bestellung_12346.pdf
│   ├── Rahmenvertrag_12346.pdf
│   └── Zeichnung_12346.pdf
├── 0000012347/
│   ├── Auftrag_12347.pdf
│   └── Lieferschein_12347.pdf
└── 0000012348/
    └── Order_12348.pdf
```

### VBAK.xlsx columns

`VBELN` | `ERDAT` | `KUNNR` | `NAME1` | `NETWR` | `WAERK` | `VKORG` | `VTWEG` | `SPART`

### VBAP.xlsx columns

`VBELN` | `POSNR` | `MATNR` | `ARKTX` | `KWMENG` | `VRKME` | `NETPR` | `MWSBP`

---

## Example Mapping Config

Upload all three files as sources, then use this mapping:

```json
{
  "sources": [
    { "id": "s_vbak",   "artifactSourceId": "<vbak-sheet-id>",   "alias": "headers" },
    { "id": "s_vbap",   "artifactSourceId": "<vbap-sheet-id>",   "alias": "positions" },
    { "id": "s_orders", "artifactSourceId": "<zip-folder-id>",   "alias": "documents" }
  ],
  "primarySourceId": "s_vbak",
  "connectors": [
    {
      "type": "column_map",
      "sourceId": "s_vbak",
      "fields": [
        { "column": "VBELN", "path": "input.order_number",   "role": "input" },
        { "column": "ERDAT", "path": "input.order_date",     "role": "input" },
        { "column": "NAME1", "path": "input.customer_name",  "role": "input" },
        { "column": "NETWR", "path": "input.net_value",      "role": "input",
          "transform": { "type": "float" } },
        { "column": "WAERK", "path": "input.currency",       "role": "input" }
      ]
    },
    {
      "type": "join",
      "primarySourceId": "s_vbak",
      "secondarySourceId": "s_vbap",
      "primaryKey": "VBELN",
      "foreignKey": "VBELN",
      "joinType": "one_to_many",
      "arrayPath": "input.line_items",
      "fields": [
        { "column": "POSNR",  "path": "position",     "role": "input" },
        { "column": "MATNR",  "path": "material",     "role": "input" },
        { "column": "ARKTX",  "path": "description",  "role": "input" },
        { "column": "KWMENG", "path": "quantity",     "role": "input",
          "transform": { "type": "float" } },
        { "column": "VRKME",  "path": "unit",         "role": "input" },
        { "column": "NETPR",  "path": "unit_price",   "role": "input",
          "transform": { "type": "float" } }
      ]
    },
    {
      "type": "join",
      "primarySourceId": "s_vbak",
      "secondarySourceId": "s_orders",
      "primaryKey": "VBELN",
      "foreignKey": "__folderName",
      "joinType": "left",
      "fields": [
        { "column": "__files", "path": "input.documents", "role": "input" }
      ]
    }
  ]
}
```

This produces test cases like:

```json
{
  "input": {
    "order_number": "0000012345",
    "order_date": "2024-03-01",
    "customer_name": "Müller Maschinenbau GmbH",
    "net_value": 15200.0,
    "currency": "EUR",
    "line_items": [
      { "position": "000010", "material": "MAT-STAHL-001", "description": "Stahlplatte 10mm S235", "quantity": 50.0, "unit": "ST", "unit_price": 185.0 },
      { "position": "000020", "material": "MAT-SCHRAUBE-M8", "description": "Schraube M8x20 DIN 933", "quantity": 500.0, "unit": "ST", "unit_price": 0.48 },
      { "position": "000030", "material": "MAT-DICHT-001", "description": "Dichtung EPDM 50x50", "quantity": 100.0, "unit": "ST", "unit_price": 1.2 }
    ],
    "documents": [
      { "type": "local", "key": "...", "filename": "Angebot_12345.pdf", ... },
      { "type": "local", "key": "...", "filename": "Spezifikation_12345.pdf", ... }
    ]
  }
}
```

## Regenerating

```bash
cd application/runtime/tests/testfiles
python generate.py
```
