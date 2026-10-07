// This source code is a part of Project Violet.
// Copyright (C) 2020-2024. violet-team. Licensed under the Apache-2.0 License.

import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:firebase_crashlytics/firebase_crashlytics.dart';
import 'package:http/http.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:sqflite/sqflite.dart';
import 'package:violet/component/query_translate.dart';
import 'package:violet/database/database.dart';
import 'package:violet/database/query.dart';
import 'package:violet/log/log.dart';
import 'package:violet/network/wrapper.dart' as http;
import 'package:violet/settings/settings.dart';

typedef DoubleIntCallback = Future Function(int, int);

class SyncInfoRecord {
  final String type;
  final int timestamp;
  final String url;
  final int size;

  SyncInfoRecord({
    required this.type,
    required this.timestamp,
    required this.url,
    this.size = 0,
  });

  String getDBDownloadUrl(String type) =>
      url + SyncManager.createRawdbPostfix(type);
  String getDBDownloadUrliOS(String type) =>
      url + SyncManager.createRawdbPostfixiOS(type);

  DateTime getDateTime() {
    return DateTime.fromMillisecondsSinceEpoch(timestamp * 1000);
  }
}

class SyncManager {
  static String syncInfoURL(String branch) {
  return 'http://129.225.133.125/syncversion.txt';
}

  static bool firstSync = false;
  static bool syncRequire = false; // database sync require
  static bool chunkRequire = false;
  static const ignoreUserAcceptThreshold = 1024 * 1024 * 10; // 10MB
  static List<SyncInfoRecord>? _rows;
  static int requestSize = 0;

  static Future<void> checkSyncLatest(bool propagateException) async {
    await checkSync('main', propagateException);
  }

  static Future<void> checkSyncOld(bool propagateException) async {
    await checkSync('main', propagateException);
  }

  static Future<void> checkSync(String branch, bool propagateException) async {
    try {
      final prefs = await SharedPreferences.getInstance();
      var latest = prefs.getInt('synclatest') ?? 0;

      syncRequire = false;
      chunkRequire = false;
      firstSync = false;

      var res = await http.get(syncInfoURL(branch));
      if (res.statusCode != 200) return;

      var lines = const LineSplitter().convert(const Utf8Decoder().convert(res.bodyBytes));
      _rows = [];

      for (var line in lines) {
        if (line.trim().isEmpty) continue;
        var match = _syncVersionPattern.firstMatch(line.trim());
        if (match != null) {
          var row = SyncInfoRecord(
            type: match.group(1)!,
            timestamp: int.parse(match.group(2)!),
            url: match.group(3)!,
            size: int.parse(match.group(4)!),
          );
          // 기기에 저장된 최신 타임스탬프(latest)보다 더 최신 청크만 추가
          if (row.type == 'chunk' && row.timestamp > latest) {
            _rows!.add(row);
          }
        }
      }

      // 새로 내려받아야 할 청크가 남아있을 때만 chunkRequire 활성화
      if (_rows!.isNotEmpty) {
        chunkRequire = true;
      }
    } catch (e, st) {
      Logger.error('[Sync-Check] E: $e\n$st');
      if (propagateException) rethrow;
    }
  }

  static Future<void> doChunkSync(
      Future<void> Function(int, int) progressCallback) async {
    if (_rows == null || _rows!.isEmpty) return;

    var filteredIter = _rows!.where((element) => element.type == 'chunk').toList();
    if (filteredIter.isEmpty) return;

    try {
      // 1. 새 청크 JSON 파일 다운로드
      var jsons = <String>[];
      for (int i = 0; i < filteredIter.length; i++) {
        var row = filteredIter[i];
        var res = await http.get(row.url);
        await progressCallback(i + 1, filteredIter.length);
        if (res.statusCode == 200) {
          jsons.add(const Utf8Decoder().convert(res.bodyBytes));
        }
      }


      // 2. DB 삽입 및 Published -> DateTime 매핑
      var db = await DataBaseManager.getInstance();
      var dbtxn = db.db!;

      await dbtxn.transaction((txn) async {
        final batch = txn.batch();
        for (var jsonStr in jsons) {
          var list = jsonDecode(jsonStr) as List<dynamic>;
          for (var item in list) {
            var map = Map<String, dynamic>.from(item as Map);
            if (map['DateTime'] == null && map['Published'] != null) {
              map['DateTime'] = map['Published'];
            }
            batch.insert(
              'HitomiColumnModel',
              map,
              conflictAlgorithm: ConflictAlgorithm.replace,
            );
          }
        }
        await batch.commit(noResult: true);
      });

      // 3. 최신 타임스탬프 영구 저장 및 플러시 (다음 실행 시 재다운로드 방지)
      final prefs = await SharedPreferences.getInstance();
      int maxTimestamp = filteredIter
          .map((e) => e.timestamp)
          .reduce((a, b) => a > b ? a : b);
      await prefs.setInt('synclatest', maxTimestamp);
      await prefs.reload();

      // 4. 검색 인덱스 갱신
      try {
        await dbtxn.execute(
          "INSERT INTO HitomiColumnModelTextSearch(HitomiColumnModelTextSearch) VALUES('rebuild');",
        );
      } catch (e) {
        Logger.error('[Sync-FTS-Rebuild] E: $e');
      }
    } catch (e, st) {
      Logger.error('[Sync-chunk] E: $e\n$st');
    }
  }

 static String createRawdbPostfix(String lang) {
    switch (lang) {
      case 'global':
        return '.7z';
      case 'ko':
      case 'korean':
        return '.7z';
      case 'zh':
        return '-chinese.7z';
      case 'ja':
        return '-japanese.7z';
      case 'en':
        return '-english.7z';
    }

    throw Exception('not reachable');
  }

 static String createRawdbPostfixiOS(String lang) {
    switch (lang) {
      case 'global':
        return '.db';
      case 'ko':
      case 'korean':
        return '.db';
      case 'zh':
        return '-chinese.db';
      case 'ja':
        return '-japanese.db';
      case 'en':
        return '-english.db';
    }

    throw Exception('not reachable');
  }

  static String translateToLanguage(String lang) {
    switch (lang) {
      case 'global':
        return '';
      case 'ko':
        return 'korean';
      case 'zh':
        return 'chinese';
      case 'ja':
        return 'japanese';
      case 'en':
        return 'english';
    }

    throw Exception('not reachable');
  }
}
