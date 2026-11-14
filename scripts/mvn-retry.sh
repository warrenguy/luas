#!/usr/bin/env bash
# Retry mvn only when Maven fails to resolve an artifact. Maven Central
# sometimes returns not-found for an artifact it holds. Maven records the
# miss in the local repository. -U makes a retry ask again. A compile or
# test failure fails at once.
set -euo pipefail

command -v mvn >/dev/null || {
	echo "luas: mvn required (JDK 26)" >&2
	exit 1
}

log="$(mktemp)"
trap 'rm -f "${log}"' EXIT

resolve_failure='Could not (resolve|find artifact|transfer artifact)|could not be resolved'

update=()
for attempt in 1 2 3; do
	status=0
	mvn -Dstyle.color=never "${update[@]}" "$@" 2>&1 | tee "${log}" || status="${PIPESTATUS[0]}"
	if [[ ${status} -eq 0 ]]; then
		exit 0
	fi

	if ! grep -qE "^\[ERROR\] .*(${resolve_failure})" "${log}"; then
		exit "${status}"
	fi

	if [[ ${attempt} -eq 3 ]]; then
		echo "luas: mvn failed to resolve artifacts after ${attempt} attempts" >&2
		exit "${status}"
	fi

	echo "luas: mvn attempt ${attempt} failed to resolve artifacts; retrying" >&2
	update=(-U)
	sleep 10
done
