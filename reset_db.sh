#!/usr/bin/env bash
sqlite3 backend.db < model.sql
sqlite3 backend.db < test/insert_test_data.sql