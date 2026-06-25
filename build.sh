#!/bin/sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SEATUNNEL_HOME="$SCRIPT_DIR/../apache-seatunnel-2.3.13"
VERSION="2.3.13"
ARTIFACT="connector-dts-${VERSION}.jar"

cd "$SCRIPT_DIR"
echo "[build] mvn package..."
mvn -q package -DskipTests

echo "[build] deploy jar -> $SEATUNNEL_HOME/connectors/$ARTIFACT"
cp "connector-dts/target/${ARTIFACT}" "$SEATUNNEL_HOME/connectors/"

echo "[build] deploy dts-sdk.jar -> plugins/connector-dts/"
mkdir -p "$SEATUNNEL_HOME/plugins/connector-dts"
cp "$SCRIPT_DIR/../dts-bridge/dts-sdk.jar" "$SEATUNNEL_HOME/plugins/connector-dts/"

MAPPING_FILE="$SEATUNNEL_HOME/connectors/plugin-mapping.properties"
if ! grep -q '^seatunnel.source.Dts' "$MAPPING_FILE"; then
  echo "seatunnel.source.Dts = connector-dts" >> "$MAPPING_FILE"
  echo "[build] appended plugin-mapping: seatunnel.source.Dts"
fi

PLUGIN_CONFIG="$SEATUNNEL_HOME/config/plugin_config"
if ! grep -q '^connector-dts$' "$PLUGIN_CONFIG"; then
  awk '/^--connectors-v2--$/ { print; print "connector-dts"; next }1' "$PLUGIN_CONFIG" > "${PLUGIN_CONFIG}.tmp"
  mv "${PLUGIN_CONFIG}.tmp" "$PLUGIN_CONFIG"
  echo "[build] appended plugin_config: connector-dts"
fi

echo "[build] done."
