(function () {
  "use strict";

  const API_ROOT = "/api";
  const PREFERRED_CONVERSATION_PAGE_SIZE = 100;
  const AUTO_LOAD_OLDER_MIN_THRESHOLD = 320;
  const TRANSIENT_FETCH_MAX_RETRIES = 30;
  const TRANSIENT_FETCH_MIN_DELAY_MS = 250;
  const TRANSIENT_FETCH_MAX_DELAY_MS = 5000;
  const MESSAGE_DETAIL_TIMEOUT_MS = 15000;
  const ATTACHMENT_PREVIEW_TIMEOUT_MS = 30000;
  const INLINE_ATTACHMENT_TIMEOUT_MS = 30000;
  const INLINE_ATTACHMENT_CACHE_MAX_ENTRIES = 12;
  const INLINE_ATTACHMENT_CACHE_MAX_BYTES = 32 * 1024 * 1024;
  const ALLOWED_IMAGE_TYPES = new Set([
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "image/avif"
  ]);

  const numberFormatter = new Intl.NumberFormat("zh-CN");
  const dateFormatter = new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
  const shortDateFormatter = new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  });
  const dayFormatter = new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "long",
    day: "numeric"
  });

  const state = {
    overview: null,
    pagination: {
      configured: false,
      defaultPageSize: 25,
      maxPageSize: 25,
      conversationPageSize: 25
    },
    overviewController: null,
    overviewRequest: 0,
    searchTimer: null,
    sessions: {
      items: [],
      page: 1,
      pageSize: 25,
      total: 0,
      hasMore: false,
      truncated: false,
      query: "",
      sourceType: "",
      loading: false,
      loaded: false,
      error: null,
      controller: null,
      request: 0,
      expandedGroups: new Set(),
      groupLimit: 5
    },
    selection: {
      key: null,
      session: null
    },
    restore: {
      key: null,
      loading: false,
      controller: null
    },
    conversation: {
      sessionKey: null,
      items: [],
      page: 1,
      pageSize: 25,
      total: 0,
      hasMore: false,
      truncated: false,
      mode: "idle",
      error: null,
      olderError: null,
      controller: null,
      request: 0,
      autoLoadFrame: null,
      restoringScroll: false,
      expandedKey: null,
      showTools: false
    },
    detailCache: new Map(),
    detailController: null,
    detailRequest: 0,
    inlineAttachmentCache: new Map(),
    inlineAttachmentCacheBytes: 0,
    inlineAttachmentQueue: [],
    inlineAttachmentController: null,
    inlineAttachmentObserver: null,
    inlineAttachmentGeneration: 0,
    inlineAttachmentPumpGeneration: null,
    previewController: null,
    previewCacheUrl: null,
    previewReturnFocus: null
  };

  const dom = {
    explorer: byId("data-explorer"),
    collectorState: byId("collector-state"),
    collectorStateText: byId("collector-state-text"),
    liveBadge: byId("live-badge"),
    liveBadgeText: byId("live-badge-text"),
    generatedAt: byId("generated-at"),
    globalError: byId("global-error"),
    globalErrorMessage: byId("global-error-message"),
    uploadedSessions: byId("uploaded-sessions"),
    uploadedMessages: byId("uploaded-messages"),
    pendingSessions: byId("pending-sessions"),
    pendingMessages: byId("pending-messages"),
    sessionsTableCard: byId("sessions-table-card"),
    messagesTableCard: byId("messages-table-card"),
    lastScanAt: byId("last-scan-at"),
    lastCommitAt: byId("last-commit-at"),
    progress: byId("upload-progress"),
    progressLabel: byId("upload-progress-label"),
    databaseName: byId("database-name"),
    sessionsTableName: byId("sessions-table-name"),
    messagesTableName: byId("messages-table-name"),
    refreshAll: byId("refresh-all"),
    sessionSearch: byId("session-search"),
    sessionSourceFilter: byId("session-source-filter"),
    sessionCount: byId("session-count"),
    sessionListStatus: byId("session-list-status"),
    sessionList: byId("session-list"),
    sessionLoading: byId("session-loading"),
    sessionEmpty: byId("session-empty"),
    sessionError: byId("session-error"),
    sessionErrorMessage: byId("session-error-message"),
    retrySessions: byId("retry-sessions"),
    loadMoreSessions: byId("load-more-sessions"),
    chatPane: byId("chat-pane"),
    chatEmpty: byId("chat-empty"),
    chatView: byId("chat-view"),
    chatBack: byId("chat-back"),
    chatTitle: byId("chat-title"),
    chatSource: byId("chat-source"),
    chatStatus: byId("chat-status"),
    chatPath: byId("chat-path"),
    chatSessionId: byId("chat-session-id"),
    refreshChat: byId("refresh-chat"),
    messageSummary: byId("message-summary"),
    showToolsToggle: byId("show-tools-toggle"),
    chatScroll: byId("chat-scroll"),
    olderMessagesLoading: byId("older-messages-loading"),
    loadOlderMessages: byId("load-older-messages"),
    messageList: byId("message-list"),
    messageLoading: byId("message-loading"),
    messageEmpty: byId("message-empty"),
    messageEmptyTitle: byId("message-empty-title"),
    messageEmptyDescription: byId("message-empty-description"),
    messageError: byId("message-error"),
    messageErrorMessage: byId("message-error-message"),
    retryMessages: byId("retry-messages"),
    chatFooterCount: byId("chat-footer-count"),
    chatLiveStatus: byId("chat-live-status"),
    toastRegion: byId("toast-region"),
    previewModal: byId("preview-modal"),
    previewTitle: byId("preview-title"),
    previewImage: byId("preview-image"),
    previewLoading: byId("preview-loading"),
    previewMeta: byId("preview-meta"),
    closePreview: byId("close-preview")
  };

  wireEvents();
  renderSessions();
  renderConversation();
  initializeDashboard();

  async function initializeDashboard() {
    // Keep initial rendering cheap: selecting a session is the user's explicit action and may
    // require a substantially larger Paimon message scan.
    await loadSessions({ reset: true });
    await loadOverview();
  }

  function byId(id) {
    return document.getElementById(id);
  }

  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) {
      node.className = className;
    }
    if (text !== undefined && text !== null) {
      node.textContent = String(text);
    }
    return node;
  }

  function hasValue(value) {
    return value !== null && value !== undefined && value !== "";
  }

  function displayValue(value, fallback) {
    if (!hasValue(value)) {
      return fallback === undefined ? "—" : fallback;
    }
    if (typeof value === "object") {
      try {
        return JSON.stringify(value);
      } catch (error) {
        return String(value);
      }
    }
    return String(value);
  }

  function safeNumber(value, fallback) {
    const number = Number(value);
    return Number.isFinite(number) && number >= 0 ? number : (fallback || 0);
  }

  function positiveWholeNumber(value, fallback) {
    const number = Number(value);
    return Number.isFinite(number) && number > 0 ? Math.floor(number) : fallback;
  }

  function formatCount(value) {
    return numberFormatter.format(safeNumber(value, 0));
  }

  function formatDate(value, short) {
    if (!hasValue(value)) {
      return "—";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return displayValue(value);
    }
    return (short ? shortDateFormatter : dateFormatter).format(date).replace(/\//g, "-");
  }

  function formatDay(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? "日期未知" : dayFormatter.format(date);
  }

  function dayKey(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return "unknown";
    }
    return [date.getFullYear(), date.getMonth() + 1, date.getDate()].join("-");
  }

  function setTime(node, value, short) {
    node.textContent = formatDate(value, short);
    const date = new Date(value);
    if (hasValue(value) && !Number.isNaN(date.getTime())) {
      node.dateTime = date.toISOString();
      node.title = formatDate(value, false);
    } else {
      node.removeAttribute("datetime");
      node.removeAttribute("title");
    }
  }

  function shortId(value) {
    const text = displayValue(value);
    if (text.length <= 24) {
      return text;
    }
    return text.slice(0, 10) + "…" + text.slice(-8);
  }

  function shortPath(value) {
    const path = displayValue(value, "未知工作目录");
    const parts = path.split(/[\\/]/).filter(Boolean);
    if (parts.length <= 2) {
      return path;
    }
    return "…/" + parts.slice(-2).join("/");
  }

  function formatBytes(value) {
    const bytes = safeNumber(value, 0);
    if (!bytes) {
      return "0 B";
    }
    const units = ["B", "KB", "MB", "GB"];
    const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const result = bytes / Math.pow(1024, index);
    return (result >= 10 || index === 0 ? result.toFixed(0) : result.toFixed(1)) + " " + units[index];
  }

  function sameOriginUrl(value) {
    if (!hasValue(value)) {
      throw new Error("附件没有可用的预览地址");
    }
    const url = new URL(String(value), window.location.href);
    if (url.origin !== window.location.origin || (url.protocol !== "http:" && url.protocol !== "https:")) {
      throw new Error("已阻止非同源附件地址");
    }
    return url.href;
  }

  async function sameOriginFetch(url, options) {
    const requestUrl = sameOriginUrl(url);
    const requestOptions = Object.assign({}, options || {});
    requestOptions.cache = "no-store";
    return window.fetch(requestUrl, requestOptions);
  }

  async function fetchJson(path, signal) {
    let transientRetries = 0;
    while (true) {
      const response = await sameOriginFetch(path, {
        method: "GET",
        headers: { Accept: "application/json" },
        signal: signal
      });
      let payload = null;
      try {
        payload = await response.json();
      } catch (error) {
        if (response.ok) {
          throw new Error("服务返回了无法识别的数据格式");
        }
      }
      if (!response.ok) {
        const retryAfter = response.headers.get("Retry-After");
        const transientBusy =
          (response.status === 429 || response.status === 503) && hasValue(retryAfter);
        if (transientBusy && transientRetries < TRANSIENT_FETCH_MAX_RETRIES) {
          const retryDelay = transientRetryDelay(retryAfter, transientRetries);
          transientRetries += 1;
          await waitForRetry(retryDelay, signal);
          continue;
        }
        let message = payload && (payload.message || payload.error);
        if (!message) {
          message = "请求失败（HTTP " + response.status + "）";
        }
        if (response.status === 403) {
          message = "请求被拒绝，请确认正在通过本机地址访问。";
        }
        const requestError = new Error(displayValue(message));
        requestError.status = response.status;
        throw requestError;
      }
      return payload && typeof payload === "object" ? payload : {};
    }
  }

  async function postRestore(path, signal) {
    const response = await sameOriginFetch(path, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "X-Paimon-Agent-Action": "restore-session"
      },
      signal: signal
    });
    let payload = null;
    try {
      payload = await response.json();
    } catch (error) {
      if (response.ok) {
        throw new Error("服务返回了无法识别的数据格式");
      }
    }
    if (!response.ok) {
      let message = payload && (payload.message || payload.error);
      if (!message) {
        message = "同步失败（HTTP " + response.status + "）";
      }
      if (response.status === 403) {
        message = "同步请求被拒绝，请确认正在通过本机地址访问。";
      }
      const requestError = new Error(displayValue(message));
      requestError.status = response.status;
      throw requestError;
    }
    return payload && typeof payload === "object" ? payload : {};
  }

  function transientRetryDelay(retryAfter, retryNumber) {
    const seconds = Number(retryAfter);
    if (Number.isFinite(seconds) && seconds >= 0) {
      return explicitRetryDelay(seconds * 1000);
    }
    const retryAt = Date.parse(retryAfter);
    if (Number.isFinite(retryAt)) {
      return explicitRetryDelay(Math.max(0, retryAt - Date.now()));
    }
    return boundedRetryDelay(TRANSIENT_FETCH_MIN_DELAY_MS * Math.pow(2, retryNumber));
  }

  function explicitRetryDelay(value) {
    return Math.max(TRANSIENT_FETCH_MIN_DELAY_MS, Math.round(value));
  }

  function boundedRetryDelay(value) {
    return Math.min(
      TRANSIENT_FETCH_MAX_DELAY_MS,
      Math.max(TRANSIENT_FETCH_MIN_DELAY_MS, Math.round(value))
    );
  }

  function waitForRetry(delay, signal) {
    return new Promise(function (resolve, reject) {
      if (signal && signal.aborted) {
        reject(abortedRequestError());
        return;
      }
      const timer = window.setTimeout(function () {
        if (signal) {
          signal.removeEventListener("abort", abortRetry);
        }
        resolve();
      }, delay);
      function abortRetry() {
        window.clearTimeout(timer);
        signal.removeEventListener("abort", abortRetry);
        reject(abortedRequestError());
      }
      if (signal) {
        signal.addEventListener("abort", abortRetry, { once: true });
      }
    });
  }

  function abortedRequestError() {
    const error = new Error("请求已取消");
    error.name = "AbortError";
    return error;
  }

  function friendlyError(error) {
    if (!error) {
      return "未知错误，请稍后重试。";
    }
    if (error.status === 403) {
      return "请求被拒绝，请确认正在通过本机地址访问。";
    }
    if (error.status === 404) {
      return "这个会话已不存在或尚未上传，请刷新会话列表后重试。";
    }
    if (error.status === 409) {
      return "请先完全退出 Codex/Claude；若客户端已退出，会话可能仍在上传，或另一个本地同步正在进行。";
    }
    if (error.status === 429 || error.status === 503) {
      return "数据读取任务较多，请稍后重试。";
    }
    if (error instanceof TypeError) {
      return "无法连接采集服务，请确认服务仍在运行。";
    }
    return displayValue(error.message, "数据读取失败，请稍后重试。");
  }

  async function loadOverview(options) {
    const opts = options || {};
    if (state.overviewController) {
      state.overviewController.abort();
    }
    const controller = new AbortController();
    const request = ++state.overviewRequest;
    state.overviewController = controller;
    try {
      const path = API_ROOT + "/overview" + (opts.refresh ? "?refresh=true" : "");
      const payload = await fetchJson(path, controller.signal);
      if (request !== state.overviewRequest) {
        return false;
      }
      state.overview = payload;
      renderOverview(payload);
      return true;
    } catch (error) {
      if (error.name === "AbortError" || request !== state.overviewRequest) {
        return false;
      }
      renderOverviewUnavailable(error);
      return false;
    }
  }

  function renderOverview(overview) {
    configurePagination(overview.defaultPageSize, overview.maxPageSize);
    const running = overview.collectorRunning === true || overview.collectorRunning === "true";
    dom.collectorState.classList.toggle("is-stopped", !running);
    dom.collectorStateText.textContent = running ? "正在运行" : "当前已停止";
    dom.liveBadge.classList.toggle("is-stopped", !running);
    dom.liveBadgeText.textContent = running ? "运行中" : "已停止";

    setTime(dom.generatedAt, overview.generatedAt, false);
    setTime(dom.lastScanAt, overview.lastScanAt, false);
    setTime(dom.lastCommitAt, overview.lastCommitAt, false);

    dom.uploadedSessions.textContent = formatCount(overview.uploadedSessions)
      + (overview.sessionCountTruncated === true ? "+" : "");
    dom.uploadedMessages.textContent = formatCount(overview.uploadedMessages)
      + (overview.messageCountTruncated === true ? "+" : "");
    dom.pendingSessions.textContent = formatCount(overview.pendingSessions);
    dom.pendingMessages.textContent = formatCount(overview.pendingMessages);

    const sessionsTable = displayValue(overview.sessionsTable, "ai_chat_sessions");
    const messagesTable = displayValue(overview.messagesTable, "ai_chat_messages");
    dom.sessionsTableCard.textContent = sessionsTable;
    dom.messagesTableCard.textContent = messagesTable;
    dom.databaseName.textContent = displayValue(overview.database);
    dom.sessionsTableName.textContent = sessionsTable;
    dom.messagesTableName.textContent = messagesTable;

    const uploaded = safeNumber(overview.uploadedMessages, 0);
    const pending = safeNumber(overview.pendingMessages, 0);
    const total = uploaded + pending;
    const percent = total > 0 ? Math.round((uploaded / total) * 100) : 0;
    dom.progress.value = percent;
    dom.progress.textContent = percent + "%";
    dom.progressLabel.textContent = total > 0
      ? percent + "% · " + formatCount(uploaded) + " / " + formatCount(total)
      : "暂无消息";

    const lastError = displayValue(overview.lastError, "").trim();
    dom.globalError.hidden = !lastError;
    dom.globalErrorMessage.textContent = lastError;
  }

  function renderOverviewUnavailable(error) {
    dom.collectorState.classList.add("is-stopped");
    dom.collectorStateText.textContent = "状态不可用";
    dom.liveBadge.classList.add("is-stopped");
    dom.liveBadgeText.textContent = "未连接";
    showToast(friendlyError(error), true);
  }

  function configurePagination(defaultValue, maximumValue) {
    const defaultPageSize = positiveWholeNumber(defaultValue, state.pagination.defaultPageSize);
    const maxPageSize = Math.max(defaultPageSize, positiveWholeNumber(maximumValue, defaultPageSize));
    const conversationPageSize = Math.min(
      maxPageSize,
      Math.max(defaultPageSize, PREFERRED_CONVERSATION_PAGE_SIZE)
    );
    const firstConfiguration = !state.pagination.configured;
    state.pagination.configured = true;
    state.pagination.defaultPageSize = defaultPageSize;
    state.pagination.maxPageSize = maxPageSize;
    state.pagination.conversationPageSize = conversationPageSize;
    if (firstConfiguration) {
      state.sessions.pageSize = defaultPageSize;
    } else {
      state.sessions.pageSize = Math.min(state.sessions.pageSize, maxPageSize);
    }
    if (state.conversation.items.length === 0
        && state.conversation.mode !== "loading"
        && state.conversation.mode !== "loading-older") {
      state.conversation.pageSize = conversationPageSize;
    }
  }

  async function loadSessions(options) {
    const opts = options || {};
    const reset = opts.reset !== false;
    const sessions = state.sessions;
    if (sessions.controller) {
      sessions.controller.abort();
    }
    const controller = new AbortController();
    const request = ++sessions.request;
    const requestedPage = reset ? 1 : sessions.page + 1;
    sessions.controller = controller;
    sessions.loading = true;
    sessions.error = null;
    renderSessions();

    const params = new URLSearchParams();
    params.set("page", String(requestedPage));
    if (state.pagination.configured) {
      params.set("pageSize", String(sessions.pageSize));
    }
    if (sessions.sourceType) {
      params.set("sourceType", sessions.sourceType);
    }
    if (sessions.query.trim()) {
      params.set("query", sessions.query.trim());
    }
    if (opts.refresh) {
      params.set("refresh", "true");
    }

    try {
      const payload = await fetchJson(API_ROOT + "/sessions?" + params.toString(), controller.signal);
      if (request !== sessions.request) {
        return false;
      }
      const items = Array.isArray(payload.items) ? payload.items : [];
      sessions.items = reset ? uniqueSessions(items) : uniqueSessions(sessions.items.concat(items));
      sessions.page = Math.max(1, positiveWholeNumber(payload.page, requestedPage));
      sessions.pageSize = positiveWholeNumber(payload.pageSize, sessions.pageSize);
      sessions.total = Math.max(safeNumber(payload.total, sessions.items.length), sessions.items.length);
      sessions.hasMore = typeof payload.hasMore === "boolean"
        ? payload.hasMore
        : sessions.page * sessions.pageSize < sessions.total;
      sessions.truncated = payload.truncated === true;
      sessions.loaded = true;
      sessions.error = null;

      if (state.selection.key) {
        const fresh = sessions.items.find(function (item) {
          return sessionKey(item) === state.selection.key;
        });
        if (fresh) {
          state.selection.session = fresh;
        }
      }
      return true;
    } catch (error) {
      if (error.name === "AbortError" || request !== sessions.request) {
        return false;
      }
      sessions.error = friendlyError(error);
      sessions.loaded = true;
      return false;
    } finally {
      if (request === sessions.request) {
        sessions.loading = false;
        renderSessions();
        renderConversationHeader();
      }
    }
  }

  function uniqueSessions(items) {
    const rows = new Map();
    items.forEach(function (item) {
      rows.set(sessionKey(item), item);
    });
    return Array.from(rows.values());
  }

  function sessionKey(item) {
    return displayValue(item && item.sourceType, "") + "\u0000" + displayValue(item && item.sessionId, "");
  }

  function messageKey(item) {
    return [item.sourceType, item.sessionId, item.messageId, item.sequenceNo].map(function (part) {
      return displayValue(part, "");
    }).join("\u0000");
  }

  function renderSessions() {
    const sessions = state.sessions;
    const fragment = document.createDocumentFragment();
    groupSessionsByProject(sessions.items).forEach(function (group) {
      fragment.appendChild(buildSessionGroup(group));
    });
    dom.sessionList.replaceChildren(fragment);

    const suffix = sessions.truncated ? "+" : "";
    dom.sessionCount.textContent = formatCount(sessions.total) + suffix + " 个会话";
    dom.sessionLoading.hidden = !sessions.loading || sessions.items.length > 0;
    dom.sessionError.hidden = sessions.loading || !sessions.error || sessions.items.length > 0;
    dom.sessionEmpty.hidden = sessions.loading || Boolean(sessions.error) || !sessions.loaded || sessions.items.length > 0;
    dom.sessionErrorMessage.textContent = sessions.error || "";
    dom.loadMoreSessions.hidden = !sessions.loaded || (!sessions.hasMore && !sessions.error);
    dom.loadMoreSessions.disabled = sessions.loading;
    dom.loadMoreSessions.textContent = sessions.loading && sessions.items.length > 0
      ? "正在加载…"
      : (sessions.error ? "重试加载会话" : "加载更多会话");

    if (sessions.loading) {
      dom.sessionListStatus.textContent = sessions.items.length > 0 ? "正在加载更多会话" : "正在读取会话";
    } else if (sessions.error) {
      dom.sessionListStatus.textContent = "会话读取失败";
    } else if (sessions.loaded) {
      dom.sessionListStatus.textContent = "已显示 " + formatCount(sessions.items.length) + " 个会话";
    } else {
      dom.sessionListStatus.textContent = "尚未读取会话";
    }
  }

  function groupSessionsByProject(items) {
    const groups = new Map();
    let tasks = null;
    items.forEach(function (item) {
      const cwd = displayValue(item.cwd, "").trim().replace(/[\\/]+$/, "");
      if (isStandaloneTaskSession(item, cwd)) {
        if (!tasks) {
          tasks = {
            key: "\u0000tasks",
            cwd: "",
            label: "Tasks",
            isTasks: true,
            items: []
          };
        }
        tasks.items.push(item);
        return;
      }
      const key = cwd || "\u0000other";
      let group = groups.get(key);
      if (!group) {
        const parts = cwd.split(/[\\/]/).filter(Boolean);
        group = {
          key: key,
          cwd: cwd,
          label: parts.length > 0 ? parts[parts.length - 1] : "其他会话",
          items: []
        };
        groups.set(key, group);
      }
      group.items.push(item);
    });
    const projectGroups = Array.from(groups.values());
    return tasks ? projectGroups.concat(tasks) : projectGroups;
  }

  function isStandaloneTaskSession(item, cwd) {
    if (displayValue(item && item.sourceType, "").trim().toLowerCase() !== "codex") {
      return false;
    }
    const normalizedCwd = displayValue(cwd, "").trim().replace(/[\\/]+$/, "");
    if (item && item.projectless === true) {
      return true;
    }
    if (!normalizedCwd) {
      return true;
    }
    // Codex creates these dated workspaces for conversations that are not attached to a project.
    if (/(?:^|[\\/])Documents[\\/]Codex[\\/]\d{4}-\d{2}-\d{2}[\\/][^\\/]+$/.test(normalizedCwd)) {
      return true;
    }
    if (item && item.projectless === false) {
      return false;
    }
    return false;
  }

  function buildSessionGroup(group) {
    const wrapper = element("li", "project-group" + (group.isTasks ? " is-tasks" : ""));
    const section = element("section", "project-section");
    section.setAttribute("aria-label", group.label);

    const heading = element("div", "project-heading" + (group.isTasks ? " task-heading" : ""));
    const title = element("h2", "project-title", group.label);
    title.title = group.cwd || group.label;
    if (group.isTasks) {
      heading.appendChild(title);
    } else {
      const folder = element("span", "project-folder");
      folder.setAttribute("aria-hidden", "true");
      heading.append(folder, title);
    }

    const list = element("ul", "project-session-list");
    const selectedIndex = group.items.findIndex(function (item) {
      return sessionKey(item) === state.selection.key;
    });
    const expanded = state.sessions.expandedGroups.has(group.key);
    const visibleCount = expanded
      ? group.items.length
      : Math.max(state.sessions.groupLimit, selectedIndex + 1);
    group.items.slice(0, visibleCount).forEach(function (item) {
      list.appendChild(buildSessionItem(item));
    });

    section.append(heading, list);
    if (visibleCount < group.items.length) {
      const more = element("button", "project-show-more", "Show more");
      more.type = "button";
      more.setAttribute("aria-label", "展开 " + group.label + " 的更多会话");
      more.addEventListener("click", function () {
        state.sessions.expandedGroups.add(group.key);
        renderSessions();
      });
      section.appendChild(more);
    }
    wrapper.appendChild(section);
    return wrapper;
  }

  function buildSessionItem(item) {
    const key = sessionKey(item);
    const active = key === state.selection.key;
    const listItem = element("li", "session-item" + (active ? " is-active" : ""));
    const button = element("button", "session-button");
    button.type = "button";
    button.dataset.sessionKey = key;
    if (active) {
      button.setAttribute("aria-current", "page");
    }
    const status = sessionStatus(item);
    const titleText = displayValue(item.title, "未命名会话");
    button.setAttribute("aria-label", titleText + "，" + sourceLabel(item.sourceType) + "，" + status.label);
    button.title = [titleText, displayValue(item.cwd, ""), formatDate(item.lastMessageAt || item.updatedAt, false)]
      .filter(Boolean)
      .join("\n");
    button.addEventListener("click", function () { selectSession(item); });

    const title = element("span", "session-item-title", titleText);
    const indicator = element("span", "session-state-dot is-" + status.kind);
    indicator.setAttribute("aria-hidden", "true");
    button.append(title, indicator);

    const syncButton = element("button", "session-sync-button");
    const restoring = state.restore.loading && state.restore.key === key;
    const available = status.kind === "uploaded";
    syncButton.type = "button";
    syncButton.disabled = state.restore.loading || !available;
    syncButton.classList.toggle("is-syncing", restoring);
    syncButton.setAttribute(
      "aria-label",
      (restoring ? "正在同步" : "同步回本地") + "：" + titleText
    );
    syncButton.title = restoring
      ? "正在同步到本机 " + sourceLabel(item.sourceType)
      : (available
        ? "请先退出 " + sourceLabel(item.sourceType) + "；同步后重启客户端即可看到会话"
        : "上传完成后才能同步回本地");
    const syncSymbol = element("span", "session-sync-symbol");
    syncSymbol.setAttribute("aria-hidden", "true");
    syncButton.append(
      syncSymbol,
      element("span", "session-sync-label", restoring ? "同步中…" : "同步回本地")
    );
    syncButton.addEventListener("click", function (event) {
      event.stopPropagation();
      restoreSessionToLocal(item);
    });
    listItem.append(button, syncButton);
    return listItem;
  }

  async function restoreSessionToLocal(item) {
    const key = sessionKey(item);
    if (state.restore.loading) {
      return;
    }
    if (sessionStatus(item).kind !== "uploaded") {
      showToast("会话上传完成后才能同步回本地", true);
      return;
    }
    const controller = new AbortController();
    state.restore.key = key;
    state.restore.loading = true;
    state.restore.controller = controller;
    renderSessions();
    try {
      const params = new URLSearchParams();
      params.set("sourceType", displayValue(item.sourceType, ""));
      params.set("sessionId", displayValue(item.sessionId, ""));
      const result = await postRestore(
        API_ROOT + "/sessions/restore?" + params.toString(),
        controller.signal
      );
      if (result.status === "restored" && safeNumber(result.restoredSessions, 0) > 0) {
        const client = sourceLabel(result.sourceType || item.sourceType);
        const restoredSessions = formatCount(safeNumber(result.restoredSessions, 0));
        const restoredMessages = formatCount(safeNumber(result.restoredMessages, 0));
        showToast(
          "已同步 " + restoredSessions + " 个关联会话、" + restoredMessages
            + " 条消息到 " + displayValue(result.target, "本机 " + client)
            + "；重启 " + client + " 后可见"
        );
      } else {
        showToast("本地已存在相同会话或记录仍在提交，本次未覆盖", true);
      }
    } catch (error) {
      if (error.name !== "AbortError") {
        showToast(friendlyError(error), true);
      }
    } finally {
      if (state.restore.key === key) {
        state.restore.key = null;
        state.restore.loading = false;
        state.restore.controller = null;
        renderSessions();
      }
    }
  }

  async function selectSession(item, options) {
    const opts = options || {};
    const key = sessionKey(item);
    const changed = key !== state.selection.key;
    state.selection.key = key;
    state.selection.session = item;
    dom.refreshChat.classList.remove("is-refreshing");
    if (changed) {
      closePreview();
      clearInlineAttachmentResources();
      cancelMessageDetail();
      state.detailCache.clear();
      prepareConversationSelection(key);
    }
    if (opts.openMobile !== false) {
      document.body.classList.add("is-chat-open");
      dom.explorer.classList.add("is-chat-open");
    }
    renderSessions();
    renderConversation();
    if (opts.announce !== false) {
      dom.chatLiveStatus.textContent = "已打开会话：" + displayValue(item.title, "未命名会话");
    }
    if (opts.openMobile !== false && window.matchMedia("(max-width: 760px)").matches) {
      window.requestAnimationFrame(function () { dom.chatTitle.focus({ preventScroll: true }); });
    }
    if (!state.pagination.configured) {
      await loadOverview();
      if (key !== state.selection.key) {
        return false;
      }
    }
    return loadMessages({ reset: true });
  }

  function prepareConversationSelection(key) {
    const conversation = state.conversation;
    cancelAutoLoadFrame();
    if (conversation.controller) {
      conversation.controller.abort();
    }
    conversation.controller = null;
    conversation.request += 1;
    conversation.sessionKey = key;
    conversation.items = [];
    conversation.page = 1;
    conversation.total = 0;
    conversation.hasMore = false;
    conversation.truncated = false;
    conversation.mode = "loading";
    conversation.error = null;
    conversation.olderError = null;
    conversation.restoringScroll = false;
    conversation.expandedKey = null;
  }

  function leaveConversation() {
    document.body.classList.remove("is-chat-open");
    dom.explorer.classList.remove("is-chat-open");
    const selected = dom.sessionList.querySelector("button[aria-current='page']");
    if (selected) {
      selected.focus({ preventScroll: true });
    } else {
      dom.sessionSearch.focus();
    }
  }

  async function loadMessages(options) {
    const session = state.selection.session;
    if (!session) {
      return false;
    }
    const opts = options || {};
    const reset = opts.reset !== false;
    const conversation = state.conversation;
    if (!reset && (conversation.mode === "loading"
        || conversation.mode === "loading-older"
        || conversation.restoringScroll
        || !conversation.hasMore)) {
      return false;
    }
    if (reset) {
      cancelAutoLoadFrame();
    }
    if (conversation.controller) {
      conversation.controller.abort();
    }
    if (reset && state.pagination.configured) {
      conversation.pageSize = state.pagination.conversationPageSize;
    }
    const controller = new AbortController();
    const request = ++conversation.request;
    const selectedKey = state.selection.key;
    const requestedPage = reset ? 1 : conversation.page + 1;
    conversation.controller = controller;
    conversation.sessionKey = selectedKey;
    conversation.mode = reset ? "loading" : "loading-older";
    conversation.error = null;
    conversation.olderError = null;
    if (reset) {
      conversation.restoringScroll = false;
      conversation.items = [];
      conversation.page = 1;
      conversation.total = 0;
      conversation.hasMore = false;
      conversation.truncated = false;
      conversation.expandedKey = null;
    }
    renderConversation();

    try {
      const params = new URLSearchParams();
      params.set("page", String(requestedPage));
      if (state.pagination.configured) {
        params.set("pageSize", String(conversation.pageSize));
      }
      params.set("sourceType", displayValue(session.sourceType, ""));
      params.set("sessionId", displayValue(session.sessionId, ""));
      if (!conversation.showTools) {
        params.set("conversationOnly", "true");
      }
      if (opts.refresh) {
        params.set("refresh", "true");
      }

      const payload = await fetchJson(API_ROOT + "/messages?" + params.toString(), controller.signal);
      if (request !== conversation.request || selectedKey !== state.selection.key) {
        return false;
      }
      const pageItems = Array.isArray(payload.items) ? payload.items.slice().reverse() : [];
      const merged = reset ? pageItems : pageItems.concat(conversation.items);
      conversation.items = uniqueMessages(merged).sort(compareMessages);
      conversation.page = Math.max(1, positiveWholeNumber(payload.page, requestedPage));
      conversation.pageSize = positiveWholeNumber(payload.pageSize, conversation.pageSize);
      conversation.total = Math.max(
        safeNumber(payload.total, conversation.items.length),
        conversation.items.length
      );
      conversation.hasMore = typeof payload.hasMore === "boolean"
        ? payload.hasMore
        : conversation.page * conversation.pageSize < conversation.total;
      conversation.truncated = conversation.truncated || payload.truncated === true;

      conversation.error = null;
      conversation.olderError = null;
      conversation.mode = conversation.items.length > 0 ? "ready" : "empty";
      const anchor = !reset ? captureScrollAnchor() : null;
      conversation.restoringScroll = !reset;
      renderConversation();
      if (reset) {
        scrollChatToBottom(request, selectedKey);
      } else {
        restoreScrollAnchor(anchor, request, selectedKey);
        dom.chatLiveStatus.textContent = "已加载更早的消息，当前显示 " + formatCount(conversation.items.length) + " 条";
      }
      return true;
    } catch (error) {
      if (error.name === "AbortError" || request !== conversation.request || selectedKey !== state.selection.key) {
        return false;
      }
      if (!reset && conversation.items.length > 0) {
        conversation.error = null;
        conversation.olderError = friendlyError(error);
        conversation.mode = "ready";
        dom.chatLiveStatus.textContent = "更早的消息加载失败，可在聊天顶部重试";
      } else {
        conversation.error = friendlyError(error);
        conversation.olderError = null;
        conversation.mode = "error";
      }
      renderConversation();
      return false;
    } finally {
      if (request === conversation.request) {
        conversation.controller = null;
      }
    }
  }

  function uniqueMessages(items) {
    const rows = new Map();
    items.forEach(function (item) {
      rows.set(messageKey(item), item);
    });
    return Array.from(rows.values());
  }

  function compareMessages(left, right) {
    const leftSequence = Number(left.sequenceNo);
    const rightSequence = Number(right.sequenceNo);
    if (Number.isFinite(leftSequence) && Number.isFinite(rightSequence) && leftSequence !== rightSequence) {
      return leftSequence - rightSequence;
    }
    const leftTime = new Date(left.createdAt || left.ingestedAt || 0).getTime();
    const rightTime = new Date(right.createdAt || right.ingestedAt || 0).getTime();
    if (leftTime !== rightTime) {
      return leftTime - rightTime;
    }
    return displayValue(left.messageId, "").localeCompare(displayValue(right.messageId, ""));
  }

  function captureScrollAnchor() {
    const containerTop = dom.chatScroll.getBoundingClientRect().top;
    const visibleMessage = Array.from(dom.messageList.querySelectorAll(".message-item")).find(function (item) {
      return item.getBoundingClientRect().bottom > containerTop;
    });
    const anchor = {
      height: dom.chatScroll.scrollHeight,
      top: dom.chatScroll.scrollTop,
      messageKey: null,
      messageOffset: 0
    };
    if (visibleMessage) {
      anchor.messageKey = visibleMessage.dataset.messageKey || null;
      anchor.messageOffset = visibleMessage.getBoundingClientRect().top - containerTop;
    }
    return anchor;
  }

  function isCurrentConversationRequest(request, selectedKey) {
    return request === state.conversation.request
      && selectedKey === state.selection.key
      && selectedKey === state.conversation.sessionKey;
  }

  function restoreScrollAnchor(anchor, request, selectedKey) {
    if (!anchor) {
      if (isCurrentConversationRequest(request, selectedKey)) {
        state.conversation.restoringScroll = false;
      }
      return;
    }
    window.requestAnimationFrame(function () {
      if (!isCurrentConversationRequest(request, selectedKey)) {
        return;
      }
      const anchoredMessage = anchor.messageKey
        ? Array.from(dom.messageList.querySelectorAll(".message-item")).find(function (item) {
          return item.dataset.messageKey === anchor.messageKey;
        })
        : null;
      if (anchoredMessage) {
        const containerTop = dom.chatScroll.getBoundingClientRect().top;
        const currentOffset = anchoredMessage.getBoundingClientRect().top - containerTop;
        dom.chatScroll.scrollTop += currentOffset - anchor.messageOffset;
      } else {
        dom.chatScroll.scrollTop = dom.chatScroll.scrollHeight - anchor.height + anchor.top;
      }
      window.requestAnimationFrame(function () {
        if (!isCurrentConversationRequest(request, selectedKey)) {
          return;
        }
        state.conversation.restoringScroll = false;
        maybeAutoLoadOlderMessages();
      });
    });
  }

  function scrollChatToBottom(request, selectedKey) {
    window.requestAnimationFrame(function () {
      if (!isCurrentConversationRequest(request, selectedKey)) {
        return;
      }
      dom.chatScroll.scrollTop = dom.chatScroll.scrollHeight;
      window.requestAnimationFrame(function () {
        if (isCurrentConversationRequest(request, selectedKey)) {
          maybeAutoLoadOlderMessages();
        }
      });
    });
  }

  function scheduleAutoLoadOlderMessages() {
    if (state.conversation.autoLoadFrame !== null) {
      return;
    }
    state.conversation.autoLoadFrame = window.requestAnimationFrame(function () {
      state.conversation.autoLoadFrame = null;
      maybeAutoLoadOlderMessages();
    });
  }

  function cancelAutoLoadFrame() {
    if (state.conversation.autoLoadFrame !== null) {
      window.cancelAnimationFrame(state.conversation.autoLoadFrame);
      state.conversation.autoLoadFrame = null;
    }
  }

  function maybeAutoLoadOlderMessages() {
    const conversation = state.conversation;
    if (!state.selection.session
        || !conversation.hasMore
        || conversation.mode !== "ready"
        || conversation.restoringScroll
        || conversation.olderError) {
      return;
    }
    const threshold = Math.max(
      AUTO_LOAD_OLDER_MIN_THRESHOLD,
      dom.chatScroll.clientHeight * 0.5
    );
    if (dom.chatScroll.scrollTop <= threshold) {
      loadMessages({ reset: false });
    }
  }

  function renderConversation() {
    const hasSession = Boolean(state.selection.session);
    dom.chatEmpty.hidden = hasSession;
    dom.chatView.hidden = !hasSession;
    if (!hasSession) {
      return;
    }
    renderConversationHeader();
    renderMessages();
    renderMessageStates();
  }

  function renderConversationHeader() {
    const session = state.selection.session;
    if (!session) {
      return;
    }
    dom.chatTitle.textContent = displayValue(session.title, "未命名会话");
    dom.chatSource.textContent = sourceLabel(session.sourceType);
    dom.chatSource.className = "source-badge " + sourceClass(session.sourceType);
    const status = sessionStatus(session);
    dom.chatStatus.textContent = status.label;
    dom.chatStatus.className = "status-badge is-" + status.kind;
    dom.chatPath.textContent = displayValue(session.cwd, "未知工作目录");
    dom.chatPath.title = displayValue(session.cwd, "未知工作目录");
    dom.chatSessionId.textContent = shortId(session.sessionId);
    dom.chatSessionId.title = displayValue(session.sessionId);
  }

  function renderMessages() {
    const fragment = document.createDocumentFragment();
    let lastDay = null;
    visibleConversationItems().forEach(function (item) {
      const currentDay = dayKey(item.createdAt || item.ingestedAt);
      if (currentDay !== lastDay) {
        const divider = element("li", "chat-day-divider");
        divider.setAttribute("aria-label", formatDay(item.createdAt || item.ingestedAt));
        divider.appendChild(element("span", "", formatDay(item.createdAt || item.ingestedAt)));
        fragment.appendChild(divider);
        lastDay = currentDay;
      }
      fragment.appendChild(buildMessageItem(item));
    });
    dom.messageList.replaceChildren(fragment);
    activateInlineAttachmentThumbnails();
  }

  function visibleConversationItems() {
    return state.conversation.items.filter(function (item) {
      return state.conversation.showTools || !isToolEvent(item);
    });
  }

  function isToolEvent(item) {
    if (normalizeRole(item.role) === "tool") {
      return true;
    }
    const event = displayValue(item.eventType, "").trim().toLowerCase().replace(/[-.]/g, "_");
    return event.includes("tool")
      || event.includes("function_call")
      || /(^|_)call(_|$)/.test(event)
      || event.endsWith("_call")
      || event.endsWith("_call_output");
  }

  function buildMessageItem(item) {
    const key = messageKey(item);
    const role = normalizeRole(item.role);
    const conversational = isConversationalMessage(item, role);
    const hasAttachments = safeNumber(item.attachmentCount, 0) > 0;
    const expanded = hasAttachments && state.conversation.expandedKey === key;
    const detailId = "message-detail-" + hashKey(key);
    const listItem = element(
      "li",
      "message-item " + (conversational ? "is-" + role : "is-event") + (expanded ? " is-expanded" : "")
    );
    listItem.dataset.messageKey = key;
    const article = element("article", "message-card");
    article.setAttribute("aria-label", roleLabel(role, item.sourceType) + "，" + formatDate(item.createdAt || item.ingestedAt, false));

    const head = element("header", "message-head");
    const identity = element("span", "message-role");
    identity.append(roleBadge(role, item.sourceType));
    if (!conversational) {
      identity.appendChild(eventBadge(item.eventType));
    }
    const time = element("time");
    setTime(time, item.createdAt || item.ingestedAt, true);
    head.append(identity, time);

    const preview = messagePreview(item);
    const inlineAttachments = displayAttachments(item);

    const meta = element("div", "message-meta");
    meta.append(
      element("span", "message-sequence", "#" + displayValue(item.sequenceNo, "—")),
      statusBadge(messageStatus(item))
    );

    article.appendChild(head);
    if (inlineAttachments.length > 0) {
      article.appendChild(buildInlineAttachments(inlineAttachments, Boolean(preview)));
    }
    if (preview) {
      const bubble = element("div", "message-bubble");
      const content = element("div", "message-content");
      renderRichText(content, preview);
      bubble.appendChild(content);
      article.appendChild(bubble);
    }
    article.appendChild(meta);
    if (hasAttachments) {
      const actions = element("div", "message-actions");
      const detailButton = element("button", "message-detail-button", expanded ? "收起原始记录" : "原始记录");
      detailButton.type = "button";
      detailButton.dataset.messageKey = key;
      detailButton.setAttribute("aria-expanded", String(expanded));
      detailButton.setAttribute("aria-controls", detailId);
      detailButton.addEventListener("click", function () { toggleMessageDetail(item, key); });
      actions.appendChild(detailButton);
      article.appendChild(actions);
    }
    if (expanded) {
      article.appendChild(buildMessageDetailArea(item, key, detailId));
    }
    listItem.appendChild(article);
    return listItem;
  }

  function messagePreview(item) {
    const preview = cleanCodexAttachmentEnvelope(displayValue(item.contentPreview, ""), item);
    if (!preview) {
      if (displayAttachments(item).length > 0) {
        return "";
      }
      return emptyMessageLabel(item);
    }
    return preview;
  }

  function displayAttachments(item) {
    if (normalizeRole(item.role) !== "user") {
      return [];
    }
    const attachments = Array.isArray(item.attachments) ? item.attachments : [];
    return attachments.filter(function (attachment) {
      return attachment.present !== false;
    });
  }

  function canPreviewInlineAttachment(attachment) {
    const mimeType = displayValue(attachment.mimeType, "").trim().toLowerCase();
    const fileName = displayValue(attachment.fileName, "").trim().toLowerCase();
    return hasValue(attachment.previewUrl)
      && (ALLOWED_IMAGE_TYPES.has(mimeType)
        || /\.(?:png|jpe?g|gif|webp|avif)$/.test(fileName));
  }

  function cleanCodexAttachmentEnvelope(value, item) {
    let text = displayValue(value, "").replace(/\r\n?/g, "\n").trim();
    if (displayValue(item && item.sourceType, "").toLowerCase() !== "codex"
        || normalizeRole(item && item.role) !== "user") {
      return text;
    }
    const filesMarker = "# Files mentioned by the user:";
    const requestMarker = "## My request for Codex:";
    const filesIndex = text.indexOf(filesMarker);
    const requestIndex = text.indexOf(requestMarker, filesIndex + filesMarker.length);
    if (filesIndex < 0 || requestIndex < 0) {
      return text;
    }
    const fileList = text.slice(filesIndex + filesMarker.length, requestIndex);
    if (!/^##\s+.+:\s+(?:\/(?!\/)|~\/|[A-Za-z]:[\\/])/m.test(fileList)) {
      return text;
    }
    text = text.slice(requestIndex + requestMarker.length);
    text = text.replace(/(?:\s*·\s*)?<image\b[^>]*>[\s\S]*?<\/image>(?:\s*·\s*)?/gi, "\n");
    text = text.replace(/(?:\s*·\s*)?<image\b[^>]*>[\s\S]*$/gi, "\n");
    text = text.replace(/(?:^|\n)\s*<\/?image\b[^>]*>\s*(?=\n|$)/gi, "\n");
    return text.replace(/^(?:\s*·\s*)+|(?:\s*·\s*)+$/g, "").trim();
  }

  function buildInlineAttachments(attachments, hasFollowingBubble) {
    const strip = element(
      "div",
      "message-inline-attachments" + (hasFollowingBubble ? " has-following-bubble" : "")
    );
    strip.setAttribute("aria-label", attachments.length + " 个附件");
    attachments.forEach(function (attachment, position) {
      const rawFileName = displayValue(
        attachment.fileName,
        "图片 " + (safeNumber(attachment.index, position) + 1)
      );
      const fileName = rawFileName.split(/[\\/]/).filter(Boolean).pop() || rawFileName;
      const mimeType = displayValue(attachment.mimeType, "image/png").trim().toLowerCase();
      if (!canPreviewInlineAttachment(attachment)) {
        const unavailable = element("div", "message-inline-attachment is-unavailable");
        unavailable.setAttribute("role", "img");
        unavailable.setAttribute("aria-label", "附件 " + fileName + " 暂不可预览");
        const placeholder = element("span", "message-inline-attachment-placeholder");
        const extension = fileName.includes(".")
          ? fileName.split(".").pop().slice(0, 5).toUpperCase()
          : "FILE";
        placeholder.appendChild(element("span", "message-inline-attachment-filetype", extension));
        unavailable.appendChild(placeholder);
        strip.appendChild(unavailable);
        return;
      }
      const button = element("button", "message-inline-attachment is-loading");
      button.type = "button";
      button.disabled = true;
      button.dataset.previewUrl = displayValue(attachment.previewUrl, "");
      button.dataset.mimeType = mimeType;
      button.dataset.fileName = fileName;
      button.setAttribute("aria-label", "预览附件 " + fileName);
      button.title = fileName;

      const image = document.createElement("img");
      image.alt = "";
      image.hidden = true;
      image.decoding = "async";
      image.addEventListener("load", function () {
        if (image.naturalWidth > 0 && image.naturalHeight > 0) {
          image.hidden = false;
          button.classList.remove("is-loading", "is-error");
          button.disabled = false;
          enforceInlineAttachmentCacheLimit(null);
        } else {
          showInlineAttachmentError(button);
        }
      });
      image.addEventListener("error", function () { showInlineAttachmentError(button); });
      const placeholder = element("span", "message-inline-attachment-placeholder");
      placeholder.setAttribute("aria-hidden", "true");
      button.append(image, placeholder);
      button.addEventListener("click", function () {
        openAttachmentPreview(attachment, fileName, mimeType, button);
      });
      strip.appendChild(button);
    });
    return strip;
  }

  function activateInlineAttachmentThumbnails() {
    if (state.inlineAttachmentObserver) {
      state.inlineAttachmentObserver.disconnect();
      state.inlineAttachmentObserver = null;
    }
    const buttons = dom.messageList.querySelectorAll(".message-inline-attachment[data-preview-url]");
    if (!buttons.length) {
      return;
    }
    if (typeof window.IntersectionObserver !== "function") {
      buttons.forEach(queueInlineAttachmentThumbnail);
      return;
    }
    const observer = new window.IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (!entry.isIntersecting) {
          return;
        }
        observer.unobserve(entry.target);
        queueInlineAttachmentThumbnail(entry.target);
      });
    }, { root: dom.chatScroll, rootMargin: "180px 0px" });
    state.inlineAttachmentObserver = observer;
    buttons.forEach(function (button) {
      const cached = touchInlineAttachmentCache(button.dataset.previewUrl);
      if (cached && cached.status === "ready") {
        showInlineAttachmentThumbnail(button, cached.objectUrl);
      } else if (cached && cached.status === "error") {
        showInlineAttachmentError(button);
      } else {
        observer.observe(button);
      }
    });
  }

  function queueInlineAttachmentThumbnail(button) {
    const previewUrl = button.dataset.previewUrl;
    if (!previewUrl) {
      showInlineAttachmentError(button);
      return;
    }
    const cached = touchInlineAttachmentCache(previewUrl);
    if (cached && cached.status === "ready") {
      showInlineAttachmentThumbnail(button, cached.objectUrl);
      return;
    }
    if (cached && (cached.status === "queued" || cached.status === "loading")) {
      return;
    }
    button.classList.add("is-loading");
    button.classList.remove("is-error");
    button.disabled = true;
    state.inlineAttachmentCache.set(previewUrl, { status: "queued" });
    state.inlineAttachmentQueue.push({
      previewUrl: previewUrl,
      mimeType: button.dataset.mimeType,
      generation: state.inlineAttachmentGeneration
    });
    pumpInlineAttachmentQueue(state.inlineAttachmentGeneration);
  }

  async function pumpInlineAttachmentQueue(generation) {
    if (state.inlineAttachmentPumpGeneration === generation) {
      return;
    }
    state.inlineAttachmentPumpGeneration = generation;
    try {
      while (generation === state.inlineAttachmentGeneration && state.inlineAttachmentQueue.length > 0) {
        const queued = state.inlineAttachmentQueue.shift();
        if (!queued || queued.generation !== generation) {
          continue;
        }
        const controller = new AbortController();
        state.inlineAttachmentController = controller;
        state.inlineAttachmentCache.set(queued.previewUrl, { status: "loading" });
        let timedOut = false;
        const timeout = window.setTimeout(function () {
          timedOut = true;
          controller.abort();
        }, INLINE_ATTACHMENT_TIMEOUT_MS);
        try {
          const result = await fetchAttachmentBlob(
            { previewUrl: queued.previewUrl, mimeType: queued.mimeType },
            controller.signal
          );
          if (generation !== state.inlineAttachmentGeneration) {
            continue;
          }
          const objectUrl = storeInlineAttachmentBlob(
            queued.previewUrl,
            result.blob,
            result.mimeType
          );
          updateInlineAttachmentThumbnails(queued.previewUrl, objectUrl, false);
        } catch (error) {
          if (generation !== state.inlineAttachmentGeneration || (error.name === "AbortError" && !timedOut)) {
            continue;
          }
          state.inlineAttachmentCache.set(queued.previewUrl, {
            status: "error",
            error: timedOut ? "缩略图读取超时" : friendlyError(error)
          });
          updateInlineAttachmentThumbnails(queued.previewUrl, null, true);
        } finally {
          window.clearTimeout(timeout);
          if (state.inlineAttachmentController === controller) {
            state.inlineAttachmentController = null;
          }
        }
      }
    } finally {
      if (state.inlineAttachmentPumpGeneration === generation) {
        state.inlineAttachmentPumpGeneration = null;
      }
    }
  }

  function updateInlineAttachmentThumbnails(previewUrl, objectUrl, failed) {
    dom.messageList.querySelectorAll(".message-inline-attachment[data-preview-url]").forEach(function (button) {
      if (button.dataset.previewUrl !== previewUrl) {
        return;
      }
      if (failed) {
        showInlineAttachmentError(button);
      } else {
        showInlineAttachmentThumbnail(button, objectUrl);
      }
    });
  }

  function showInlineAttachmentThumbnail(button, objectUrl) {
    const image = button.querySelector("img");
    if (!image || !objectUrl) {
      showInlineAttachmentError(button);
      return;
    }
    image.src = objectUrl;
    image.hidden = true;
    button.classList.add("is-loading");
    button.classList.remove("is-error");
    if (image.complete) {
      if (image.naturalWidth > 0 && image.naturalHeight > 0) {
        image.hidden = false;
        button.classList.remove("is-loading");
        button.disabled = false;
        enforceInlineAttachmentCacheLimit(null);
      } else {
        showInlineAttachmentError(button);
      }
    }
  }

  function storeInlineAttachmentBlob(previewUrl, blob, mimeType) {
    const previous = state.inlineAttachmentCache.get(previewUrl);
    if (previous && previous.status === "ready" && previous.objectUrl) {
      URL.revokeObjectURL(previous.objectUrl);
      state.inlineAttachmentCacheBytes = Math.max(
        0,
        state.inlineAttachmentCacheBytes - safeNumber(previous.size, 0)
      );
    }
    const objectUrl = URL.createObjectURL(blob);
    const entry = {
      status: "ready",
      objectUrl: objectUrl,
      mimeType: mimeType,
      size: blob.size,
      lastUsed: Date.now()
    };
    state.inlineAttachmentCache.set(previewUrl, entry);
    state.inlineAttachmentCacheBytes += blob.size;
    enforceInlineAttachmentCacheLimit(previewUrl);
    return objectUrl;
  }

  function touchInlineAttachmentCache(previewUrl) {
    const cached = state.inlineAttachmentCache.get(previewUrl);
    if (cached && cached.status === "ready") {
      cached.lastUsed = Date.now();
    }
    return cached;
  }

  function enforceInlineAttachmentCacheLimit(keepUrl) {
    while (readyInlineAttachmentCount() > INLINE_ATTACHMENT_CACHE_MAX_ENTRIES
        || state.inlineAttachmentCacheBytes > INLINE_ATTACHMENT_CACHE_MAX_BYTES) {
      let oldestUrl = null;
      let oldestEntry = null;
      state.inlineAttachmentCache.forEach(function (entry, previewUrl) {
        if (previewUrl === keepUrl
            || previewUrl === state.previewCacheUrl
            || !entry
            || entry.status !== "ready") {
          return;
        }
        if (!oldestEntry || safeNumber(entry.lastUsed, 0) < safeNumber(oldestEntry.lastUsed, 0)) {
          oldestUrl = previewUrl;
          oldestEntry = entry;
        }
      });
      if (!oldestEntry) {
        break;
      }
      evictInlineAttachment(oldestUrl, oldestEntry);
    }
  }

  function readyInlineAttachmentCount() {
    let count = 0;
    state.inlineAttachmentCache.forEach(function (entry) {
      if (entry && entry.status === "ready") {
        count += 1;
      }
    });
    return count;
  }

  function evictInlineAttachment(previewUrl, entry) {
    URL.revokeObjectURL(entry.objectUrl);
    state.inlineAttachmentCacheBytes = Math.max(
      0,
      state.inlineAttachmentCacheBytes - safeNumber(entry.size, 0)
    );
    state.inlineAttachmentCache.delete(previewUrl);
  }

  function showInlineAttachmentError(button) {
    button.classList.remove("is-loading");
    button.classList.add("is-error");
    button.disabled = false;
    const image = button.querySelector("img");
    if (image) {
      image.hidden = true;
      image.removeAttribute("src");
    }
  }

  function clearInlineAttachmentResources() {
    state.inlineAttachmentGeneration += 1;
    if (state.inlineAttachmentObserver) {
      state.inlineAttachmentObserver.disconnect();
      state.inlineAttachmentObserver = null;
    }
    if (state.inlineAttachmentController) {
      state.inlineAttachmentController.abort();
      state.inlineAttachmentController = null;
    }
    state.inlineAttachmentQueue = [];
    state.inlineAttachmentCache.forEach(function (cached) {
      if (cached && cached.status === "ready" && cached.objectUrl) {
        URL.revokeObjectURL(cached.objectUrl);
      }
    });
    state.inlineAttachmentCache.clear();
    state.inlineAttachmentCacheBytes = 0;
  }

  function renderRichText(container, value) {
    const text = displayValue(value, "").replace(/\r\n?/g, "\n");
    const fence = /```([^\n`]*)\n?([\s\S]*?)```/g;
    let cursor = 0;
    let match;
    while ((match = fence.exec(text)) !== null) {
      appendTextBlocks(container, text.slice(cursor, match.index));
      const pre = element("pre", "message-code-block");
      const code = element("code", "", match[2].replace(/\n$/, ""));
      const language = match[1].trim();
      if (language) {
        code.dataset.language = language;
      }
      pre.appendChild(code);
      container.appendChild(pre);
      cursor = match.index + match[0].length;
    }
    appendTextBlocks(container, text.slice(cursor));
  }

  function appendTextBlocks(container, value) {
    const lines = value.split("\n");
    let plainLines = [];
    let index = 0;
    function flushPlainLines() {
      appendPlainTextBlocks(container, plainLines.join("\n"));
      plainLines = [];
    }
    while (index < lines.length) {
      const table = markdownTableAt(lines, index);
      if (!table) {
        plainLines.push(lines[index]);
        index += 1;
        continue;
      }
      flushPlainLines();
      appendMarkdownTable(container, table);
      index = table.nextIndex;
    }
    flushPlainLines();
  }

  function appendPlainTextBlocks(container, value) {
    value.split(/\n{2,}/).forEach(function (rawBlock) {
      const block = rawBlock.trim();
      if (!block) {
        return;
      }
      const lines = block.split("\n");
      if (lines.every(function (line) { return /^\s*[-*]\s+/.test(line); })) {
        const list = element("ul", "message-markdown-list");
        lines.forEach(function (line) {
          const item = element("li");
          appendInlineMarkdown(item, line.replace(/^\s*[-*]\s+/, ""));
          list.appendChild(item);
        });
        container.appendChild(list);
        return;
      }
      const heading = lines.length === 1 ? /^(#{1,3})\s+(.+)$/.exec(lines[0]) : null;
      if (heading) {
        const node = element("h" + Math.min(heading[1].length + 2, 5), "message-markdown-heading");
        appendInlineMarkdown(node, heading[2]);
        container.appendChild(node);
        return;
      }
      const paragraph = element("p");
      lines.forEach(function (line, index) {
        if (index > 0) {
          paragraph.appendChild(document.createElement("br"));
        }
        appendInlineMarkdown(paragraph, line);
      });
      container.appendChild(paragraph);
    });
  }

  function markdownTableAt(lines, index) {
    if (index + 1 >= lines.length) {
      return null;
    }
    const header = parseMarkdownTableRow(lines[index]);
    if (!header || header.length < 2) {
      return null;
    }
    const alignments = parseMarkdownTableDelimiter(lines[index + 1], header.length);
    if (!alignments) {
      return null;
    }
    const rows = [];
    let nextIndex = index + 2;
    while (nextIndex < lines.length && lines[nextIndex].trim()) {
      const row = parseMarkdownTableRow(lines[nextIndex]);
      if (!row) {
        break;
      }
      while (row.length < header.length) {
        row.push("");
      }
      rows.push(row.slice(0, header.length));
      nextIndex += 1;
    }
    return { header: header, alignments: alignments, rows: rows, nextIndex: nextIndex };
  }

  function parseMarkdownTableDelimiter(line, columnCount) {
    const cells = parseMarkdownTableRow(line);
    if (!cells || cells.length !== columnCount) {
      return null;
    }
    const alignments = [];
    for (let index = 0; index < cells.length; index++) {
      const value = cells[index].trim();
      if (!/^:?-{3,}:?$/.test(value)) {
        return null;
      }
      const left = value.startsWith(":");
      const right = value.endsWith(":");
      alignments.push(left && right ? "center" : (right ? "right" : "left"));
    }
    return alignments;
  }

  function parseMarkdownTableRow(line) {
    const source = displayValue(line, "").trim();
    if (!source || !source.includes("|")) {
      return null;
    }
    const cells = [];
    let cell = "";
    let codeFenceLength = 0;
    let hasSeparator = false;
    let leadingSeparator = false;
    let trailingSeparator = false;
    for (let index = 0; index < source.length; index++) {
      const character = source[index];
      if (character === "\\"
          && codeFenceLength === 0
          && (source[index + 1] === "|" || source[index + 1] === "`")) {
        cell += source[index + 1];
        trailingSeparator = false;
        index += 1;
        continue;
      }
      if (character === "`") {
        let runLength = 1;
        while (source[index + runLength] === "`") {
          runLength += 1;
        }
        if (codeFenceLength === 0
            && hasClosingBacktickRun(source, index + runLength, runLength)) {
          codeFenceLength = runLength;
        } else if (codeFenceLength === runLength) {
          codeFenceLength = 0;
        }
        cell += source.slice(index, index + runLength);
        trailingSeparator = false;
        index += runLength - 1;
        continue;
      }
      if (character === "|" && codeFenceLength === 0) {
        if (index === 0) {
          leadingSeparator = true;
        }
        cells.push(cell.trim());
        cell = "";
        hasSeparator = true;
        trailingSeparator = true;
        continue;
      }
      cell += character;
      trailingSeparator = false;
    }
    cells.push(cell.trim());
    if (!hasSeparator) {
      return null;
    }
    if (leadingSeparator) {
      cells.shift();
    }
    if (trailingSeparator) {
      cells.pop();
    }
    return cells;
  }

  function hasClosingBacktickRun(source, start, expectedLength) {
    let index = start;
    while (index < source.length) {
      if (source[index] !== "`" || isEscapedMarkdownCharacter(source, index)) {
        index += 1;
        continue;
      }
      let runLength = 1;
      while (source[index + runLength] === "`") {
        runLength += 1;
      }
      if (runLength === expectedLength) {
        return true;
      }
      index += runLength;
    }
    return false;
  }

  function isEscapedMarkdownCharacter(source, index) {
    let slashes = 0;
    for (let cursor = index - 1; cursor >= 0 && source[cursor] === "\\"; cursor--) {
      slashes += 1;
    }
    return slashes % 2 === 1;
  }

  function appendMarkdownTable(container, model) {
    const wrapper = element("div", "message-markdown-table-wrap");
    const table = element("table", "message-markdown-table");
    const head = element("thead");
    const headerRow = element("tr");
    model.header.forEach(function (value, index) {
      const cell = element("th", "is-align-" + model.alignments[index]);
      cell.scope = "col";
      appendInlineMarkdown(cell, value);
      headerRow.appendChild(cell);
    });
    head.appendChild(headerRow);
    table.appendChild(head);
    if (model.rows.length > 0) {
      const body = element("tbody");
      model.rows.forEach(function (row) {
        const tableRow = element("tr");
        row.forEach(function (value, index) {
          const cell = element("td", "is-align-" + model.alignments[index]);
          appendInlineMarkdown(cell, value);
          tableRow.appendChild(cell);
        });
        body.appendChild(tableRow);
      });
      table.appendChild(body);
    }
    wrapper.appendChild(table);
    container.appendChild(wrapper);
  }

  function appendInlineMarkdown(container, value) {
    const token = /`([^`\n]+)`|\*\*([^*\n]+)\*\*|\[([^\]\n]+)\]\((https?:\/\/[^)\s]+)\)/g;
    let cursor = 0;
    let match;
    while ((match = token.exec(value)) !== null) {
      if (match.index > cursor) {
        container.appendChild(document.createTextNode(value.slice(cursor, match.index)));
      }
      if (match[1] !== undefined) {
        container.appendChild(element("code", "", match[1]));
      } else if (match[2] !== undefined) {
        container.appendChild(element("strong", "", match[2]));
      } else {
        const link = element("a", "", match[3]);
        link.href = match[4];
        link.target = "_blank";
        link.rel = "noreferrer noopener";
        container.appendChild(link);
      }
      cursor = match.index + match[0].length;
    }
    if (cursor < value.length) {
      container.appendChild(document.createTextNode(value.slice(cursor)));
    }
  }

  function emptyMessageLabel(item) {
    const event = displayValue(item.eventType, "").toLowerCase();
    if (event.includes("image")) {
      return "[图片]";
    }
    if (event.includes("tool") || event.includes("call")) {
      return "工具事件";
    }
    return "（无文本内容）";
  }

  function extractHumanText(value, eventType) {
    let parsed = value;
    if (typeof value === "string") {
      try {
        parsed = JSON.parse(value);
      } catch (error) {
        return extractTextFromTruncatedJson(value);
      }
    }
    const texts = [];
    const toolNames = [];
    walkContent(parsed, texts, toolNames, new Set());
    const unique = texts.filter(function (text, index, values) {
      return text && values.indexOf(text) === index;
    });
    if (unique.length > 0) {
      return unique.join("\n\n");
    }
    if (toolNames.length > 0) {
      return "调用工具：" + toolNames.join("、");
    }
    const event = displayValue(eventType, "");
    return event ? "[" + event + "]" : "";
  }

  function walkContent(value, texts, toolNames, seen) {
    if (value === null || value === undefined || seen.has(value)) {
      return;
    }
    if (typeof value === "string") {
      return;
    }
    if (typeof value !== "object") {
      return;
    }
    seen.add(value);
    if (Array.isArray(value)) {
      value.forEach(function (item) { walkContent(item, texts, toolNames, seen); });
      return;
    }
    const type = displayValue(value.type, "").toLowerCase();
    if (typeof value.text === "string" && (type.includes("text") || type === "")) {
      texts.push(value.text.trim());
    }
    if (typeof value.content === "string" && (type === "message" || type === "user" || type === "assistant")) {
      texts.push(value.content.trim());
    }
    if (typeof value.output === "string" && (type.includes("output") || type.includes("result"))) {
      texts.push(value.output.trim());
    }
    if (typeof value.name === "string" && (type.includes("tool") || type.includes("call"))) {
      toolNames.push(value.name);
    }
    Object.keys(value).forEach(function (key) {
      if (key !== "text" && key !== "content" && key !== "output" && key !== "name") {
        walkContent(value[key], texts, toolNames, seen);
      } else if (typeof value[key] === "object") {
        walkContent(value[key], texts, toolNames, seen);
      }
    });
  }

  function extractTextFromTruncatedJson(value) {
    const texts = [];
    const pattern = /"(?:text|output)"\s*:\s*"((?:\\.|[^"\\])*)"/g;
    let match;
    while ((match = pattern.exec(value)) !== null && texts.length < 3) {
      try {
        texts.push(JSON.parse("\"" + match[1] + "\"").trim());
      } catch (error) {
        texts.push(match[1]);
      }
    }
    return texts.filter(Boolean).join("\n\n");
  }

  function renderMessageStates() {
    const conversation = state.conversation;
    const loadingInitial = conversation.mode === "loading";
    const loadingOlder = conversation.mode === "loading-older";
    const visibleItems = visibleConversationItems();
    const hiddenToolCount = conversation.items.length - visibleItems.length;
    const onlyHiddenTools = conversation.mode === "ready" && visibleItems.length === 0 && hiddenToolCount > 0;
    dom.messageLoading.hidden = !loadingInitial;
    dom.messageEmpty.hidden = conversation.mode !== "empty" && !onlyHiddenTools;
    dom.messageEmptyTitle.textContent = onlyHiddenTools ? "工具调用已隐藏" : "这个会话还没有消息";
    dom.messageEmptyDescription.textContent = onlyHiddenTools
      ? (conversation.hasMore
        ? "向上滚动会继续查找更早的对话，或打开“显示工具调用”查看这些记录。"
        : "打开“显示工具调用”即可查看这些记录。")
      : "下一次采集完成后，新消息会显示在这里。";
    dom.messageError.hidden = conversation.mode !== "error";
    dom.messageErrorMessage.textContent = conversation.error || "";
    dom.olderMessagesLoading.hidden = !loadingOlder;
    dom.loadOlderMessages.hidden = !conversation.olderError;
    dom.loadOlderMessages.disabled = loadingOlder;
    dom.loadOlderMessages.textContent = "加载失败，重试更早消息";
    dom.loadOlderMessages.title = conversation.olderError || "";
    dom.loadOlderMessages.setAttribute(
      "aria-label",
      conversation.olderError
        ? "加载失败，重试更早消息：" + conversation.olderError
        : "加载失败，重试更早消息"
    );
    dom.refreshChat.disabled = loadingInitial || loadingOlder;

    const suffix = conversation.truncated ? "+" : "";
    dom.messageSummary.textContent = formatCount(conversation.total) + suffix + " 条消息"
      + (hiddenToolCount > 0 && !conversation.showTools ? " · 已隐藏 " + formatCount(hiddenToolCount) + " 条工具记录" : "");
    dom.chatFooterCount.textContent = "已显示 " + formatCount(visibleItems.length) + " 条";
  }

  function toggleMessageDetail(item, key) {
    const opening = state.conversation.expandedKey !== key;
    state.conversation.expandedKey = opening ? key : null;
    renderMessages();
    if (!opening) {
      cancelMessageDetail();
      focusMessageAction(key);
      return;
    }
    const cached = state.detailCache.get(key);
    if (!cached || cached.status === "error" || (cached.status === "loading" && !state.detailController)) {
      loadMessageDetail(item, key);
    } else {
      focusMessageAction(key);
    }
  }

  async function loadMessageDetail(item, key) {
    cancelMessageDetail();
    const controller = new AbortController();
    const request = ++state.detailRequest;
    const selectedKey = state.selection.key;
    let timedOut = false;
    const timeout = window.setTimeout(function () {
      timedOut = true;
      controller.abort();
    }, MESSAGE_DETAIL_TIMEOUT_MS);
    state.detailController = controller;
    state.detailCache.set(key, { status: "loading" });
    renderMessages();
    focusMessageAction(key);

    const params = new URLSearchParams();
    params.set("sourceType", displayValue(item.sourceType, ""));
    params.set("sessionId", displayValue(item.sessionId, ""));
    params.set("messageId", displayValue(item.messageId, ""));
    params.set("sequenceNo", displayValue(item.sequenceNo, ""));
    try {
      const payload = await fetchJson(API_ROOT + "/messages/detail?" + params.toString(), controller.signal);
      if (request !== state.detailRequest || selectedKey !== state.selection.key) {
        return;
      }
      if (!sameMessageIdentity(item, payload)) {
        throw new Error("服务返回了不属于当前消息的详情");
      }
      state.detailCache.set(key, { status: "ready", data: payload });
    } catch (error) {
      if (timedOut && request === state.detailRequest && selectedKey === state.selection.key) {
        state.detailCache.set(key, { status: "error", error: "附件信息读取超时，请重试" });
      } else if (error.name === "AbortError" || request !== state.detailRequest || selectedKey !== state.selection.key) {
        return;
      } else {
        state.detailCache.set(key, { status: "error", error: friendlyError(error) });
      }
    } finally {
      window.clearTimeout(timeout);
      if (request === state.detailRequest) {
        state.detailController = null;
        if (state.conversation.expandedKey === key && selectedKey === state.selection.key) {
          const focusedMessageKey = focusedDetailButtonKey();
          renderMessages();
          if (focusedMessageKey) {
            focusMessageAction(focusedMessageKey);
          }
        }
      }
    }
  }

  function cancelMessageDetail() {
    if (state.detailController) {
      state.detailController.abort();
      state.detailController = null;
      state.detailRequest += 1;
      state.detailCache.forEach(function (cached, key) {
        if (cached.status === "loading") {
          state.detailCache.delete(key);
        }
      });
    }
  }

  function sameMessageIdentity(expected, actual) {
    return displayValue(expected.sourceType, "") === displayValue(actual.sourceType, "")
      && displayValue(expected.sessionId, "") === displayValue(actual.sessionId, "")
      && displayValue(expected.messageId, "") === displayValue(actual.messageId, "")
      && displayValue(expected.sequenceNo, "") === displayValue(actual.sequenceNo, "");
  }

  function focusMessageAction(key) {
    window.requestAnimationFrame(function () {
      const buttons = dom.messageList.querySelectorAll(".message-detail-button");
      for (let index = 0; index < buttons.length; index++) {
        if (buttons[index].dataset.messageKey === key) {
          buttons[index].focus({ preventScroll: true });
          return;
        }
      }
    });
  }

  function focusedDetailButtonKey() {
    const active = document.activeElement;
    return active && active.classList && active.classList.contains("message-detail-button")
      ? active.dataset.messageKey
      : null;
  }

  function buildMessageDetailArea(item, key, detailId) {
    const region = element("section", "message-detail");
    region.id = detailId;
    region.setAttribute("role", "region");
    region.setAttribute("aria-label", "消息详情");
    const cached = state.detailCache.get(key);
    if (!cached || cached.status === "loading") {
      const loading = element("div", "detail-loader");
      const spinner = element("span", "loader");
      spinner.setAttribute("aria-hidden", "true");
      loading.append(spinner, element("span", "", "正在读取消息正文与附件…"));
      region.appendChild(loading);
      return region;
    }
    if (cached.status === "error") {
      const error = element("div", "detail-error");
      error.appendChild(element("p", "", cached.error));
      const retry = element("button", "button button-quiet", "重试");
      retry.type = "button";
      retry.addEventListener("click", function () { loadMessageDetail(item, key); });
      error.appendChild(retry);
      region.appendChild(error);
      return region;
    }
    region.appendChild(buildMessageDetail(cached.data));
    return region;
  }

  function buildMessageDetail(item) {
    const panel = element("div", "detail-panel");
    const attachments = Array.isArray(item.attachments) ? item.attachments : [];
    const contentTruncated = item.contentTruncated === true;
    const fullText = cleanCodexAttachmentEnvelope(
      extractHumanText(item.contentJson, item.eventType),
      item
    );
    if (fullText && fullText !== messagePreview(item)) {
      const fullContent = element("section", "message-full-content");
      const rendered = element("div", "message-full-text message-content");
      renderRichText(rendered, fullText);
      fullContent.append(element("h3", "", "完整文本"), rendered);
      panel.appendChild(fullContent);
    }
    if (attachments.length > 0) {
      panel.appendChild(buildAttachments(attachments));
    }
    if (contentTruncated) {
      const notice = element("p", "content-truncated-notice", "content_json 超过页面展示上限，原始记录仅显示服务端返回的前一部分。");
      notice.setAttribute("role", "status");
      panel.appendChild(notice);
    }

    const details = element("details", "raw-record-details");
    details.appendChild(element("summary", "", "查看原始记录"));
    const grid = detailGrid([
      ["Message ID", item.messageId, true],
      ["Session ID", item.sessionId, true],
      ["来源", item.sourceType],
      ["角色", item.role],
      ["事件类型", item.eventType],
      ["序号", item.sequenceNo],
      ["创建时间", formatDate(item.createdAt, false), true],
      ["采集时间", formatDate(item.ingestedAt, false), true]
    ]);
    const jsonSections = element("div", "json-sections");
    jsonSections.append(
      jsonBlock(contentTruncated ? "content_json（已截断）" : "content_json", item.contentJson),
      jsonBlock("消息详情记录", item)
    );
    details.append(grid, jsonSections);
    panel.appendChild(details);
    return panel;
  }

  function detailGrid(fields) {
    const grid = element("div", "detail-grid");
    fields.forEach(function (field) {
      const block = element("div", "detail-field" + (field[2] ? " is-wide" : ""));
      const label = element("span", "", field[0]);
      const value = element("code", "", displayValue(field[1]));
      value.title = displayValue(field[1]);
      block.append(label, value);
      grid.appendChild(block);
    });
    return grid;
  }

  function jsonBlock(title, value) {
    const block = element("section", "json-block");
    const header = element("header");
    const label = element("span", "", title);
    const copy = element("button", "copy-button", "复制");
    copy.type = "button";
    const formatted = prettyJson(value);
    copy.addEventListener("click", function () { copyText(formatted); });
    const pre = element("pre", "", formatted);
    header.append(label, copy);
    block.append(header, pre);
    return block;
  }

  function prettyJson(value) {
    if (!hasValue(value)) {
      return "—";
    }
    let normalized = value;
    if (typeof value === "string") {
      try {
        normalized = JSON.parse(value);
      } catch (error) {
        return value;
      }
    }
    try {
      return JSON.stringify(normalized, null, 2);
    } catch (error) {
      return displayValue(value);
    }
  }

  async function copyText(text) {
    try {
      if (!navigator.clipboard || typeof navigator.clipboard.writeText !== "function") {
        throw new Error("当前浏览器不支持复制");
      }
      await navigator.clipboard.writeText(text);
      showToast("已复制到剪贴板");
    } catch (error) {
      showToast("复制失败，请手动选择文本", true);
    }
  }

  function buildAttachments(attachments) {
    const section = element("section", "attachments-section");
    const head = element("div", "attachment-head");
    head.append(element("h4", "", "附件"), element("span", "", attachments.length + " 项"));
    const grid = element("div", "attachment-grid");
    attachments.forEach(function (attachment, position) {
      grid.appendChild(attachmentCard(attachment, position));
    });
    section.append(head, grid);
    return section;
  }

  function attachmentCard(attachment, position) {
    const card = element("article", "attachment-card");
    const fileName = displayValue(attachment.fileName, "附件 " + (safeNumber(attachment.index, position) + 1));
    const mimeType = displayValue(attachment.mimeType, "application/octet-stream").toLowerCase();
    const extension = fileName.includes(".") ? fileName.split(".").pop().slice(0, 5) : "FILE";
    const canPreview = attachment.present !== false && ALLOWED_IMAGE_TYPES.has(mimeType) && hasValue(attachment.previewUrl);
    let visual;
    if (canPreview) {
      visual = element("button", "attachment-preview");
      visual.type = "button";
      visual.setAttribute("aria-label", "预览附件 " + fileName);
      visual.appendChild(element("span", "attachment-placeholder", "预览"));
      visual.addEventListener("click", function () {
        openAttachmentPreview(attachment, fileName, mimeType, visual);
      });
    } else {
      visual = element("div", "attachment-preview");
      visual.appendChild(element("span", "attachment-placeholder", extension));
    }
    card.append(visual, element("strong", "", fileName));
    const meta = element("p");
    meta.append(element("span", "", mimeType), element("span", "", formatBytes(attachment.size)));
    card.appendChild(meta);
    if (attachment.present === false) {
      card.appendChild(element("span", "unavailable-note", "附件内容不可用"));
    } else if (ALLOWED_IMAGE_TYPES.has(mimeType) && !hasValue(attachment.previewUrl)) {
      card.appendChild(element("span", "unavailable-note", "附件过大或暂不可预览"));
    }
    return card;
  }

  async function openAttachmentPreview(attachment, fileName, mimeType, trigger) {
    closePreviewResources();
    state.previewReturnFocus = trigger || document.activeElement;
    dom.previewTitle.textContent = fileName;
    dom.previewMeta.textContent = mimeType + " · " + formatBytes(attachment.size);
    dom.previewImage.hidden = true;
    dom.previewImage.removeAttribute("src");
    dom.previewImage.alt = "附件预览：" + fileName;
    dom.previewLoading.classList.remove("is-error");
    dom.previewLoading.replaceChildren(element("span", "loader"), element("strong", "", "正在安全加载附件"));
    dom.previewLoading.hidden = false;
    dom.previewModal.hidden = false;
    document.body.classList.add("has-modal");
    dom.closePreview.focus();

    const controller = new AbortController();
    let timedOut = false;
    const timeout = window.setTimeout(function () {
      timedOut = true;
      controller.abort();
    }, ATTACHMENT_PREVIEW_TIMEOUT_MS);
    state.previewController = controller;
    try {
      const previewUrl = displayValue(attachment.previewUrl, "");
      const cached = touchInlineAttachmentCache(previewUrl);
      if (controller.signal.aborted || dom.previewModal.hidden) {
        return;
      }
      if (cached && cached.status === "ready" && cached.objectUrl) {
        state.previewCacheUrl = previewUrl;
        dom.previewImage.src = cached.objectUrl;
      } else {
        const result = await fetchAttachmentBlob(attachment, controller.signal);
        if (controller.signal.aborted || dom.previewModal.hidden) {
          return;
        }
        state.previewCacheUrl = previewUrl;
        dom.previewImage.src = storeInlineAttachmentBlob(
          previewUrl,
          result.blob,
          result.mimeType
        );
        updateInlineAttachmentThumbnails(previewUrl, dom.previewImage.src, false);
      }
      await waitForPreviewImage(dom.previewImage, controller.signal);
      if (controller.signal.aborted || dom.previewModal.hidden) {
        return;
      }
      dom.previewImage.hidden = false;
      dom.previewLoading.hidden = true;
    } catch (error) {
      if (error.name === "AbortError" && !timedOut) {
        return;
      }
      if (!dom.previewModal.hidden) {
        dom.previewLoading.classList.add("is-error");
        dom.previewLoading.replaceChildren(
          element("span", "error-mark", "!"),
          element("strong", "", timedOut ? "附件读取超时，请重试" : friendlyError(error))
        );
        dom.previewLoading.hidden = false;
      }
    } finally {
      window.clearTimeout(timeout);
      if (state.previewController === controller) {
        state.previewController = null;
      }
    }
  }

  async function fetchAttachmentBlob(attachment, signal) {
    const mimeType = displayValue(attachment.mimeType, "image/png").trim().toLowerCase();
    let transientRetries = 0;
    while (true) {
      const response = await sameOriginFetch(attachment.previewUrl, {
        method: "GET",
        headers: { Accept: mimeType },
        signal: signal
      });
      if (!response.ok) {
        const retryAfter = response.headers.get("Retry-After");
        const transientBusy =
          (response.status === 429 || response.status === 503) && hasValue(retryAfter);
        if (transientBusy && transientRetries < TRANSIENT_FETCH_MAX_RETRIES) {
          const retryDelay = transientRetryDelay(retryAfter, transientRetries);
          transientRetries += 1;
          await waitForRetry(retryDelay, signal);
          continue;
        }
        const error = new Error(response.status === 403
          ? "附件请求被拒绝，请确认正在通过本机地址访问"
          : "附件读取失败（HTTP " + response.status + "）");
        error.status = response.status;
        throw error;
      }
      const responseType = (response.headers.get("Content-Type") || mimeType)
        .split(";", 1)[0]
        .trim()
        .toLowerCase();
      if (!ALLOWED_IMAGE_TYPES.has(responseType)) {
        throw new Error("该附件不是支持预览的图片格式");
      }
      return { blob: await response.blob(), mimeType: responseType };
    }
  }

  async function waitForPreviewImage(image, signal) {
    if (!image.complete) {
      await new Promise(function (resolve, reject) {
        let settled = false;
        function finish(callback, value) {
          if (settled) {
            return;
          }
          settled = true;
          image.removeEventListener("load", onLoad);
          image.removeEventListener("error", onError);
          signal.removeEventListener("abort", onAbort);
          callback(value);
        }
        function onLoad() {
          finish(resolve);
        }
        function onError() {
          finish(reject, new Error("图片解码失败"));
        }
        function onAbort() {
          finish(reject, new DOMException("附件预览已取消", "AbortError"));
        }
        if (signal.aborted) {
          onAbort();
          return;
        }
        image.addEventListener("load", onLoad, { once: true });
        image.addEventListener("error", onError, { once: true });
        signal.addEventListener("abort", onAbort, { once: true });
        if (image.complete) {
          if (image.naturalWidth > 0 && image.naturalHeight > 0) {
            onLoad();
          } else {
            onError();
          }
        }
      });
    }
    if (signal.aborted) {
      throw new DOMException("附件预览已取消", "AbortError");
    }
    if (typeof image.decode === "function") {
      await new Promise(function (resolve, reject) {
        let settled = false;
        function finish(callback, value) {
          if (settled) {
            return;
          }
          settled = true;
          signal.removeEventListener("abort", onAbort);
          callback(value);
        }
        function onAbort() {
          finish(reject, new DOMException("附件预览已取消", "AbortError"));
        }
        if (signal.aborted) {
          onAbort();
          return;
        }
        signal.addEventListener("abort", onAbort, { once: true });
        image.decode().then(
          function () { finish(resolve); },
          function () { finish(reject, new Error("图片解码失败")); }
        );
      });
    }
    if (!image.complete || image.naturalWidth <= 0 || image.naturalHeight <= 0) {
      throw new Error("图片解码失败");
    }
  }

  function closePreviewResources() {
    if (state.previewController) {
      state.previewController.abort();
      state.previewController = null;
    }
    state.previewCacheUrl = null;
    enforceInlineAttachmentCacheLimit(null);
    dom.previewImage.removeAttribute("src");
  }

  function closePreview() {
    if (dom.previewModal.hidden) {
      return;
    }
    closePreviewResources();
    dom.previewModal.hidden = true;
    document.body.classList.remove("has-modal");
    const returnFocus = state.previewReturnFocus;
    state.previewReturnFocus = null;
    if (returnFocus && typeof returnFocus.focus === "function" && document.contains(returnFocus)) {
      returnFocus.focus();
    }
  }

  function sourceLabel(value) {
    const source = displayValue(value, "unknown").toLowerCase();
    if (source === "codex") {
      return "Codex";
    }
    if (source === "claude") {
      return "Claude";
    }
    return displayValue(value, "其他");
  }

  function sourceClass(value) {
    const source = displayValue(value, "unknown").toLowerCase();
    return source === "codex" || source === "claude" ? "is-" + source : "is-unknown";
  }

  function sourceBadge(value) {
    return element("span", "source-badge " + sourceClass(value), sourceLabel(value));
  }

  function normalizeRole(value) {
    const role = displayValue(value, "unknown").toLowerCase();
    return ["user", "assistant", "system", "tool", "developer"].includes(role) ? role : "unknown";
  }

  function isConversationalMessage(item, role) {
    if (role !== "user" && role !== "assistant") {
      return false;
    }
    const eventType = displayValue(item.eventType, "").toLowerCase();
    return !eventType || eventType === "message" || eventType === "user" || eventType === "assistant";
  }

  function roleLabel(role, sourceType) {
    const labels = {
      user: "你",
      assistant: sourceLabel(sourceType),
      system: "系统",
      tool: "工具",
      developer: "开发者",
      unknown: "事件"
    };
    return labels[role] || "事件";
  }

  function roleBadge(role, sourceType) {
    const className = role === "user" || role === "assistant" ? " is-" + role : "";
    return element("span", "role-badge" + className, roleLabel(role, sourceType));
  }

  function eventBadge(value) {
    return element("span", "event-badge", displayValue(value, "message"));
  }

  function statusBadge(status) {
    return element("span", "status-badge is-" + status.kind, status.label);
  }

  function sessionStatus(item) {
    const status = displayValue(item.storageStatus, "").trim().toLowerCase();
    if (status === "pending" || hasValue(item.pendingCommitId)) {
      return { kind: "pending", label: "有待提交更新" };
    }
    if (status === "uploaded" || safeNumber(item.lastCommitId, 0) > 0 || hasValue(item.ingestedAt)) {
      return { kind: "uploaded", label: "已同步" };
    }
    return { kind: "local", label: "已采集" };
  }

  function messageStatus(item) {
    const status = displayValue(item.storageStatus, "").trim().toLowerCase();
    if (status === "pending") {
      return { kind: "pending", label: "待上传" };
    }
    if (status === "local" || status === "collected") {
      return { kind: "local", label: "已采集" };
    }
    if (status === "uploaded" || hasValue(item.ingestedAt)) {
      return { kind: "uploaded", label: "已上传" };
    }
    return { kind: "pending", label: "待上传" };
  }

  function hashKey(value) {
    let hash = 2166136261;
    for (let index = 0; index < value.length; index++) {
      hash ^= value.charCodeAt(index);
      hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0).toString(36);
  }

  async function refreshAll() {
    dom.refreshAll.disabled = true;
    dom.refreshAll.classList.add("is-refreshing");
    closePreview();
    clearInlineAttachmentResources();
    cancelMessageDetail();
    state.detailCache.clear();
    // One invalidation clears every server-side dashboard cache. The following message and
    // overview reads therefore share the newly loaded snapshot instead of invalidating it again.
    const sessionsLoaded = await loadSessions({ reset: true, refresh: true });
    let messagesLoaded = null;
    if (state.selection.session) {
      messagesLoaded = await loadMessages({ reset: true });
    }
    const overviewLoaded = await loadOverview();
    dom.refreshAll.disabled = false;
    dom.refreshAll.classList.remove("is-refreshing");
    const success = overviewLoaded || sessionsLoaded || messagesLoaded === true;
    showToast(success ? "数据已刷新" : "刷新失败，请稍后重试", !success);
  }

  async function refreshCurrentConversation() {
    if (!state.selection.session) {
      return;
    }
    const selectedKey = state.selection.key;
    dom.refreshChat.disabled = true;
    dom.refreshChat.classList.add("is-refreshing");
    closePreview();
    clearInlineAttachmentResources();
    cancelMessageDetail();
    state.detailCache.clear();
    const success = await loadMessages({ reset: true, refresh: true });
    if (selectedKey === state.selection.key) {
      dom.refreshChat.classList.remove("is-refreshing");
      renderMessageStates();
      showToast(success ? "当前会话已刷新" : "刷新失败，请稍后重试", !success);
    }
  }

  function applySessionSearch() {
    const query = dom.sessionSearch.value;
    if (query === state.sessions.query) {
      return;
    }
    state.sessions.query = query;
    loadSessions({ reset: true });
  }

  function showToast(message, isError) {
    const toast = element("div", "toast" + (isError ? " is-error" : ""), message);
    dom.toastRegion.appendChild(toast);
    window.setTimeout(function () { toast.remove(); }, 3200);
  }

  function wireEvents() {
    dom.refreshAll.addEventListener("click", refreshAll);
    dom.refreshChat.addEventListener("click", refreshCurrentConversation);
    dom.retrySessions.addEventListener("click", function () { loadSessions({ reset: true }); });
    dom.retryMessages.addEventListener("click", function () { loadMessages({ reset: true }); });
    dom.loadMoreSessions.addEventListener("click", function () {
      loadSessions({ reset: Boolean(state.sessions.error) });
    });
    dom.loadOlderMessages.addEventListener("click", function () { loadMessages({ reset: false }); });
    dom.chatScroll.addEventListener("scroll", scheduleAutoLoadOlderMessages, { passive: true });
    dom.chatBack.addEventListener("click", leaveConversation);
    dom.showToolsToggle.addEventListener("change", function () {
      state.conversation.showTools = dom.showToolsToggle.checked;
      closePreview();
      clearInlineAttachmentResources();
      cancelMessageDetail();
      state.detailCache.clear();
      dom.chatLiveStatus.textContent = state.conversation.showTools ? "已显示工具调用" : "已隐藏工具调用";
      loadMessages({ reset: true });
    });

    dom.sessionSearch.addEventListener("input", function () {
      clearTimeout(state.searchTimer);
      state.searchTimer = window.setTimeout(applySessionSearch, 500);
    });
    dom.sessionSearch.addEventListener("keydown", function (event) {
      if (event.isComposing) {
        return;
      }
      if (event.key === "Enter") {
        clearTimeout(state.searchTimer);
        applySessionSearch();
      } else if (event.key === "Escape" && dom.sessionSearch.value) {
        dom.sessionSearch.value = "";
        clearTimeout(state.searchTimer);
        applySessionSearch();
      }
    });
    dom.sessionSourceFilter.addEventListener("change", function () {
      state.sessions.sourceType = dom.sessionSourceFilter.value;
      loadSessions({ reset: true });
    });

    document.addEventListener("keydown", function (event) {
      if (!dom.previewModal.hidden) {
        if (event.key === "Escape") {
          closePreview();
        } else if (event.key === "Tab") {
          event.preventDefault();
          dom.closePreview.focus();
        }
        return;
      }
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        leaveConversation();
        dom.sessionSearch.focus();
      }
    });

    dom.closePreview.addEventListener("click", closePreview);
    dom.previewModal.querySelector("[data-close-preview]").addEventListener("click", closePreview);
    dom.previewImage.addEventListener("error", function () {
      if (!dom.previewModal.hidden) {
        closePreviewResources();
        dom.previewImage.hidden = true;
        dom.previewLoading.classList.add("is-error");
        dom.previewLoading.replaceChildren(
          element("span", "error-mark", "!"),
          element("strong", "", "图片解码失败")
        );
        dom.previewLoading.hidden = false;
      }
    });
    window.addEventListener("beforeunload", function () {
      closePreviewResources();
      clearInlineAttachmentResources();
      cancelMessageDetail();
      if (state.sessions.controller) {
        state.sessions.controller.abort();
      }
      if (state.conversation.controller) {
        state.conversation.controller.abort();
      }
      if (state.restore.controller) {
        state.restore.controller.abort();
      }
    });
  }
})();
