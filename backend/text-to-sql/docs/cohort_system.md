# Cohort System (Generic, Paper-agnostic)

## Overview
- Input: paper PDF + SchemaMap + `accuracy_mode=true`
- Output: CohortSpec + Evidence + Ambiguity questions + Compiled SQL + Validation report

## Pipeline
1. Adaptive snippet extraction (FAST/ACCURATE/STRICT)
2. CohortSpec draft + evidence guard
3. Ambiguity resolver (2~3 prioritized questions)
4. SQL compilation with anti-pattern blocking
5. Execution-based validation

## Accuracy-mode gate
- SQL generation is blocked when unresolved ambiguities exist.
- Validation pass is required before completed status.
