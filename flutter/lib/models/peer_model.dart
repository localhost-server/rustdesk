import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:flutter/foundation.dart';
import 'package:get/get.dart';
import 'platform_model.dart';
// ignore: depend_on_referenced_packages
import 'package:collection/collection.dart';

class Peer {
  final String id;
  String hash; // personal ab hash password
  String password; // shared ab password
  String username; // pc username
  String hostname;
  String platform;
  String alias;
  List<dynamic> tags;
  bool forceAlwaysRelay = false;
  String rdpPort;
  String rdpUsername;
  bool online = false;
  String loginName; //login username
  String device_group_name;
  String note;
  bool? sameServer;

  String getId() {
    if (alias != '') {
      return alias;
    }
    return id;
  }

  Peer.fromJson(Map<String, dynamic> json)
      : id = json['id'] ?? '',
        hash = json['hash'] ?? '',
        password = json['password'] ?? '',
        username = json['username'] ?? '',
        hostname = json['hostname'] ?? '',
        platform = json['platform'] ?? '',
        alias = json['alias'] ?? '',
        tags = json['tags'] ?? [],
        forceAlwaysRelay = json['forceAlwaysRelay'] == 'true',
        rdpPort = json['rdpPort'] ?? '',
        rdpUsername = json['rdpUsername'] ?? '',
        loginName = json['loginName'] ?? '',
        device_group_name = json['device_group_name'] ?? '',
        note = json['note'] is String ? json['note'] : '',
        sameServer = json['same_server'];

  Map<String, dynamic> toJson() {
    return <String, dynamic>{
      "id": id,
      "hash": hash,
      "password": password,
      "username": username,
      "hostname": hostname,
      "platform": platform,
      "alias": alias,
      "tags": tags,
      "forceAlwaysRelay": forceAlwaysRelay.toString(),
      "rdpPort": rdpPort,
      "rdpUsername": rdpUsername,
      'loginName': loginName,
      'device_group_name': device_group_name,
      'note': note,
      'same_server': sameServer,
    };
  }

  Map<String, dynamic> toCustomJson({required bool includingHash}) {
    var res = <String, dynamic>{
      "id": id,
      "username": username,
      "hostname": hostname,
      "platform": platform,
      "alias": alias,
      "tags": tags,
    };
    if (includingHash) {
      res['hash'] = hash;
    }
    return res;
  }

  Map<String, dynamic> toGroupCacheJson() {
    return <String, dynamic>{
      "id": id,
      "username": username,
      "hostname": hostname,
      "platform": platform,
      "login_name": loginName,
      "device_group_name": device_group_name,
    };
  }

  Peer({
    required this.id,
    required this.hash,
    required this.password,
    required this.username,
    required this.hostname,
    required this.platform,
    required this.alias,
    required this.tags,
    required this.forceAlwaysRelay,
    required this.rdpPort,
    required this.rdpUsername,
    required this.loginName,
    required this.device_group_name,
    required this.note,
    this.sameServer,
  });

  Peer.loading()
      : this(
          id: '...',
          hash: '',
          password: '',
          username: '...',
          hostname: '...',
          platform: '...',
          alias: '',
          tags: [],
          forceAlwaysRelay: false,
          rdpPort: '',
          rdpUsername: '',
          loginName: '',
          device_group_name: '',
          note: '',
        );
  bool equal(Peer other) {
    return id == other.id &&
        hash == other.hash &&
        password == other.password &&
        username == other.username &&
        hostname == other.hostname &&
        platform == other.platform &&
        alias == other.alias &&
        tags.equals(other.tags) &&
        forceAlwaysRelay == other.forceAlwaysRelay &&
        rdpPort == other.rdpPort &&
        rdpUsername == other.rdpUsername &&
        device_group_name == other.device_group_name &&
        loginName == other.loginName &&
        note == other.note;
  }

  factory Peer.copy(Peer other) {
    final peer = Peer(
        id: other.id,
        hash: other.hash,
        password: other.password,
        username: other.username,
        hostname: other.hostname,
        platform: other.platform,
        alias: other.alias,
        tags: other.tags.toList(),
        forceAlwaysRelay: other.forceAlwaysRelay,
        rdpPort: other.rdpPort,
        rdpUsername: other.rdpUsername,
        loginName: other.loginName,
        device_group_name: other.device_group_name,
        note: other.note,
        sameServer: other.sameServer);
    peer.online = other.online;
    return peer;
  }
}

enum UpdateEvent { online, load }

typedef GetInitPeers = RxList<Peer> Function();

class Peers extends ChangeNotifier {
  final String name;
  final String loadEvent;
  List<Peer> peers = List.empty(growable: true);
  // Part of the peers that are not in the rest peers list.
  // When there're too many peers, we may want to load the front 100 peers first,
  // so we can see peers in UI quickly. `restPeerIds` is the rest peers' ids.
  // And then load all peers later.
  List<String> restPeerIds = List.empty(growable: true);
  final GetInitPeers? getInitPeers;
  UpdateEvent event = UpdateEvent.load;
  static const _cbQueryOnlines = 'callback_query_onlines';
  final Map<String, Peer> _dartDiscoveredPeers = {};
  bool _isDartDiscovering = false;

  Peers(
      {required this.name,
      required this.getInitPeers,
      required this.loadEvent}) {
    peers = getInitPeers?.call() ?? [];
    platformFFI.registerEventHandler(_cbQueryOnlines, name, (evt) async {
      _updateOnlineState(evt);
    });
    platformFFI.registerEventHandler(loadEvent, name, (evt) async {
      _updatePeers(evt);
    });
  }

  @override
  void dispose() {
    platformFFI.unregisterEventHandler(_cbQueryOnlines, name);
    platformFFI.unregisterEventHandler(loadEvent, name);
    super.dispose();
  }

  Peer getByIndex(int index) {
    if (index < peers.length) {
      return peers[index];
    } else {
      return Peer.loading();
    }
  }

  int getPeersCount() {
    return peers.length;
  }

  void _updateOnlineState(Map<String, dynamic> evt) {
    int changedCount = 0;
    evt['onlines'].split(',').forEach((online) {
      for (var i = 0; i < peers.length; i++) {
        if (peers[i].id == online) {
          if (!peers[i].online) {
            changedCount += 1;
            peers[i].online = true;
          }
        }
      }
    });

    evt['offlines'].split(',').forEach((offline) {
      for (var i = 0; i < peers.length; i++) {
        if (peers[i].id == offline) {
          if (peers[i].online) {
            changedCount += 1;
            peers[i].online = false;
          }
        }
      }
    });

    if (changedCount > 0) {
      event = UpdateEvent.online;
      notifyListeners();
    }
  }

  void _updatePeers(Map<String, dynamic> evt) {
    final onlineStates = _getOnlineStates();
    if (getInitPeers != null) {
      peers = getInitPeers?.call() ?? [];
    } else {
      peers = _decodePeers(evt['peers']);
    }

    restPeerIds = [];
    if (evt['ids'] != null) {
      restPeerIds = (evt['ids'] as String).split(',');
    }

    for (var peer in peers) {
      final state = onlineStates[peer.id];
      peer.online = state != null && state != false;
    }
    if (loadEvent == 'load_lan_peers' && _dartDiscoveredPeers.isNotEmpty) {
      for (final dPeer in _dartDiscoveredPeers.values) {
        final idx = peers.indexWhere((p) => p.id == dPeer.id);
        if (idx >= 0) {
          peers[idx].online = true;
        } else {
          peers.insert(0, Peer.copy(dPeer));
        }
      }
    }
    event = UpdateEvent.load;
    notifyListeners();
  }

  Future<Set<String>> _getLocalSubnetPrefixes() async {
    final prefixes = <String>{};
    if (kIsWeb) return prefixes;
    try {
      final routeFile = File('/proc/net/route');
      if (await routeFile.exists()) {
        final lines = await routeFile.readAsLines();
        for (var i = 1; i < lines.length; i++) {
          final parts = lines[i].trim().split(RegExp(r'\s+'));
          if (parts.length >= 8) {
            final iface = parts[0];
            final destHex = parts[1];
            final maskHex = parts[7];
            if (iface == 'lo' ||
                iface.startsWith('dummy') ||
                destHex.length != 8 ||
                maskHex == '00000000') {
              continue;
            }
            final dest = int.tryParse(destHex, radix: 16);
            if (dest != null && dest != 0) {
              final b0 = dest & 0xff;
              final b1 = (dest >> 8) & 0xff;
              final b2 = (dest >> 16) & 0xff;
              if (b0 == 192 || b0 == 10 || b0 == 172) {
                prefixes.add('$b0.$b1.$b2.');
              }
            }
          }
        }
      }
    } catch (_) {}
    try {
      final interfaces =
          await NetworkInterface.list(type: InternetAddressType.IPv4);
      for (final iface in interfaces) {
        for (final addr in iface.addresses) {
          final parts = addr.address.split('.');
          if (parts.length == 4 &&
              (parts[0] == '192' || parts[0] == '10' || parts[0] == '172')) {
            prefixes.add('${parts[0]}.${parts[1]}.${parts[2]}.');
          }
        }
      }
    } catch (_) {}
    return prefixes;
  }

  Peer? _parseDiscoveryPong(Uint8List data, String selfId) {
    if (data.length < 6 || data[0] != 0xb2 || data[1] != 0x01) {
      return null;
    }
    int i = 2;
    int subLen = 0;
    int shift = 0;
    while (i < data.length) {
      final b = data[i++];
      subLen |= (b & 0x7f) << shift;
      if ((b & 0x80) == 0) break;
      shift += 7;
    }
    final end = (i + subLen <= data.length) ? i + subLen : data.length;
    String cmd = '';
    String id = '';
    String username = '';
    String hostname = '';
    String platform = '';
    while (i < end) {
      final tag = data[i++];
      final fieldNum = tag >> 3;
      final wireType = tag & 0x07;
      if (wireType == 2) {
        int len = 0;
        int s = 0;
        while (i < end) {
          final b = data[i++];
          len |= (b & 0x7f) << s;
          if ((b & 0x80) == 0) break;
          s += 7;
        }
        if (i + len > end) break;
        final str =
            utf8.decode(data.sublist(i, i + len), allowMalformed: true);
        i += len;
        if (fieldNum == 1) cmd = str;
        if (fieldNum == 3) id = str;
        if (fieldNum == 4) username = str;
        if (fieldNum == 5) hostname = str;
        if (fieldNum == 6) platform = str;
      } else if (wireType == 0) {
        while (i < end && (data[i++] & 0x80) != 0) {}
      } else {
        break;
      }
    }
    if (cmd != 'pong' || id.isEmpty || id == selfId) {
      return null;
    }
    final peer = Peer.fromJson({
      'id': id,
      'username': username,
      'hostname': hostname,
      'platform': platform,
    });
    peer.online = true;
    return peer;
  }

  Future<void> discoverLanPeers() async {
    if (kIsWeb || _isDartDiscovering) return;
    _isDartDiscovering = true;
    RawDatagramSocket? socket;
    try {
      final selfId = await bind.mainGetMyId();
      final prefixes = await _getLocalSubnetPrefixes();
      socket = await RawDatagramSocket.bind(InternetAddress.anyIPv4, 0);
      socket.broadcastEnabled = true;

      socket.listen((RawSocketEvent evt) {
        if (evt == RawSocketEvent.read) {
          final dg = socket?.receive();
          if (dg == null) return;
          final discovered = _parseDiscoveryPong(dg.data, selfId);
          if (discovered != null) {
            _dartDiscoveredPeers[discovered.id] = discovered;
            final idx = peers.indexWhere((p) => p.id == discovered.id);
            if (idx >= 0) {
              peers[idx].online = true;
              peers[idx].username = discovered.username;
              peers[idx].hostname = discovered.hostname;
              peers[idx].platform = discovered.platform;
            } else {
              peers.insert(0, Peer.copy(discovered));
            }
            event = UpdateEvent.load;
            notifyListeners();
          }
        }
      });

      final idBytes = utf8.encode(selfId.isNotEmpty ? selfId : '99');
      final subBody = <int>[
        0x0a,
        0x04,
        0x70,
        0x69,
        0x6e,
        0x67,
        0x1a,
        idBytes.length,
        ...idBytes,
      ];
      final pingPacket = Uint8List.fromList([
        0xb2,
        0x01,
        subBody.length,
        ...subBody,
      ]);

      const port = 21119;
      try {
        socket.send(pingPacket, InternetAddress('255.255.255.255'), port);
      } catch (_) {}

      for (final prefix in prefixes) {
        try {
          socket.send(pingPacket, InternetAddress('${prefix}255'), port);
        } catch (_) {}
        for (int host = 1; host <= 254; host++) {
          try {
            socket.send(pingPacket, InternetAddress('$prefix$host'), port);
          } catch (_) {}
        }
      }

      await Future.delayed(const Duration(milliseconds: 2500));
    } catch (_) {
    } finally {
      socket?.close();
      _isDartDiscovering = false;
    }
  }

  Map<String, bool> _getOnlineStates() {
    var onlineStates = <String, bool>{};
    for (var peer in peers) {
      onlineStates[peer.id] = peer.online;
    }
    return onlineStates;
  }

  List<Peer> _decodePeers(String peersStr) {
    try {
      if (peersStr == "") return [];
      List<dynamic> peers = json.decode(peersStr);
      return peers.map((peer) {
        return Peer.fromJson(peer as Map<String, dynamic>);
      }).toList();
    } catch (e) {
      debugPrint('peers(): $e');
    }
    return [];
  }
}
