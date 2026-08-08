#!/bin/sh
if [ "${FED_EXAMPLE_FAIL_COMPOSE_RM:-}" = "1" ]; then
  for argument in "$@"; do
    if [ "$argument" = "rm" ]; then
      echo "injected compose rm failure" >&2
      exit 42
    fi
  done
fi

exec docker "$@"
