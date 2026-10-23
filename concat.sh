#!/bin/bash

# Valors per defecte si l'usuari no passa cap argument
DIRECTORI_INICI="${1:-.}"
FITXER_SORTIDA="concat.txt"
NOM_SCRIPT=$(basename "$0")

# Esborra el fitxer de sortida si ja existia
> "$FITXER_SORTIDA"

echo "Processant el directori: $DIRECTORI_INICI"
echo "Es desarà a: $FITXER_SORTIDA"

# Cerca fitxers evitant carpetes i fitxers no desitjats
find "$DIRECTORI_INICI" \
  -type d -name ".git" -prune -o \
  -type d -name ".ruff_cache" -prune -o \
  -type d -name "tmp" -prune -o \
  -type d -name "__pycache__" -prune -o \
  -type f ! -name "$NOM_SCRIPT" \
  ! -name "$FITXER_SORTIDA" \
  ! -iname "*.md" \
  ! -name ".gitignore" \
  ! -name "concat.txt" \
  -print | while read -r fitxer; do

  # Escriu la ruta relativa com a capçalera
  echo "FITXER: $fitxer" >> "$FITXER_SORTIDA"
  echo "" >> "$FITXER_SORTIDA"
  
  # Afegeix el contingut del fitxer
  cat "$fitxer" >> "$FITXER_SORTIDA"
  
  # Afegeix salts de línia al final per seguretat
  echo -e "\n" >> "$FITXER_SORTIDA"

done

echo "Fet! Tots els fitxers s'han concatenat correctament a $FITXER_SORTIDA."
