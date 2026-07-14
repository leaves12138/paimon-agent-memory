(function () {
  "use strict";

  const API_ROOT = "/api";
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

  const state = {
    activeTab: "sessions",
    overview: null,
    pagination: {
      configured: false,
      defaultPageSize: 25,
      maxPageSize: 25
    },
    overviewController: null,
    overviewRequest: 0,
    searchTimer: null,
    detailCache: new Map(),
    previewController: null,
    previewObjectUrl: null,
    previewReturnFocus: null,
    tabs: {
      sessions: createTabState(),
      messages: Object.assign(createTabState(), {
        sessionId: "",
        role: "",
        eventType: ""
      })
    }
  };

  const dom = {
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
    recordCount: byId("record-count"),
    tablePanel: byId("table-panel"),
    tableCaption: byId("table-caption"),
    tableHead: byId("table-head"),
    tableBody: byId("table-body"),
    tableStatus: byId("table-status"),
    loadingState: byId("loading-state"),
    emptyState: byId("empty-state"),
    errorState: byId("error-state"),
    tableErrorMessage: byId("table-error-message"),
    pagination: byId("pagination"),
    paginationSummary: byId("pagination-summary"),
    pageNumbers: byId("page-numbers"),
    previousPage: byId("previous-page"),
    nextPage: byId("next-page"),
    pageSize: byId("page-size"),
    searchInput: byId("search-input"),
    sourceFilter: byId("source-filter"),
    sessionFilterChip: byId("session-filter-chip"),
    sessionFilterValue: byId("session-filter-value"),
    refreshAll: byId("refresh-all"),
    refreshTable: byId("refresh-table"),
    toastRegion: byId("toast-region"),
    previewModal: byId("preview-modal"),
    previewTitle: byId("preview-title"),
    previewImage: byId("preview-image"),
    previewLoading: byId("preview-loading"),
    previewMeta: byId("preview-meta"),
    closePreview: byId("close-preview")
  };

  const tableDefinitions = {
    sessions: [
      "来源",
      "会话",
      "工作目录",
      "最后消息",
      "状态",
      "提交版本",
      "详情"
    ],
    messages: [
      "序号",
      "来源",
      "角色",
      "消息内容",
      "事件类型",
      "采集时间",
      "状态",
      "详情"
    ]
  };

  wireEvents();
  renderTable();

  initializeDashboard();

  async function initializeDashboard() {
    await loadOverview();
    await loadTable();
  }

  function createTabState() {
    return {
      page: 1,
      pageSize: 25,
      sourceType: "",
      query: "",
      items: [],
      total: 0,
      hasMore: false,
      truncated: false,
      loading: false,
      loaded: false,
      error: null,
      expandedKey: null,
      controller: null,
      request: 0
    };
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

  async function loadOverview() {
    if (state.overviewController) {
      state.overviewController.abort();
    }
    const controller = new AbortController();
    const request = ++state.overviewRequest;
    state.overviewController = controller;
    try {
      const payload = await fetchJson(API_ROOT + "/overview", controller.signal);
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

  async function loadTable(options) {
    const tabName = state.activeTab;
    const tab = state.tabs[tabName];
    const opts = options || {};
    if (tab.controller) {
      tab.controller.abort();
    }
    const controller = new AbortController();
    const request = ++tab.request;
    tab.controller = controller;
    tab.loading = true;
    tab.error = null;
    tab.expandedKey = null;
    if (opts.clearDetails) {
      state.detailCache.clear();
    }
    renderTable();

    const params = new URLSearchParams();
    params.set("page", String(tab.page));
    if (state.pagination.configured) {
      params.set("pageSize", String(tab.pageSize));
    }
    if (tab.sourceType) {
      params.set("sourceType", tab.sourceType);
    }
    if (tab.query.trim()) {
      params.set("query", tab.query.trim());
    }
    if (tabName === "messages" && tab.sessionId) {
      params.set("sessionId", tab.sessionId);
    }
    if (tabName === "messages" && tab.role) {
      params.set("role", tab.role);
    }
    if (tabName === "messages" && tab.eventType) {
      params.set("eventType", tab.eventType);
    }

    try {
      const payload = await fetchJson(API_ROOT + "/" + tabName + "?" + params.toString(), controller.signal);
      if (request !== tab.request) {
        return false;
      }
      const items = Array.isArray(payload.items) ? payload.items : [];
      const responsePage = Math.max(1, Math.floor(safeNumber(payload.page, tab.page)));
      const responsePageSize = Math.max(1, Math.floor(safeNumber(payload.pageSize, tab.pageSize)));
      if (!state.pagination.configured) {
        configurePagination(responsePageSize, responsePageSize);
      }
      tab.items = items;
      tab.page = responsePage;
      tab.pageSize = responsePageSize;
      tab.total = Math.max(safeNumber(payload.total, items.length), items.length);
      tab.hasMore = typeof payload.hasMore === "boolean"
        ? payload.hasMore
        : tab.page * tab.pageSize < tab.total;
      tab.truncated = payload.truncated === true;
      tab.loaded = true;
      tab.error = null;
      return true;
    } catch (error) {
      if (error.name === "AbortError" || request !== tab.request) {
        return false;
      }
      tab.error = friendlyError(error);
      tab.loaded = true;
      return false;
    } finally {
      if (request === tab.request) {
        tab.loading = false;
        if (state.activeTab === tabName) {
          renderTable();
        }
      }
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
    document.querySelector("#sessions-tab code").textContent = sessionsTable;
    document.querySelector("#messages-tab code").textContent = messagesTable;

    const uploaded = safeNumber(overview.uploadedMessages, 0);
    const pending = safeNumber(overview.pendingMessages, 0);
    const total = uploaded + pending;
    const percent = total > 0 ? Math.round((uploaded / total) * 100) : 0;
    dom.progress.value = percent;
    dom.progress.textContent = percent + "%";
    dom.progressLabel.textContent = total > 0 ? percent + "% · " + formatCount(uploaded) + " / " + formatCount(total) : "暂无消息";

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

  function friendlyError(error) {
    if (!error) {
      return "未知错误，请稍后重试。";
    }
    if (error.status === 403) {
      return "请求被拒绝，请确认正在通过本机地址访问。";
    }
    if (error instanceof TypeError) {
      return "无法连接采集服务，请确认服务仍在运行。";
    }
    return displayValue(error.message, "数据读取失败，请稍后重试。");
  }

  function renderTable() {
    const tabName = state.activeTab;
    const tab = state.tabs[tabName];
    renderTableHead(tabName);
    renderRows(tabName, tab);
    syncControls(tabName, tab);
    renderTableStates(tabName, tab);
    renderPagination(tab);
  }

  function renderTableHead(tabName) {
    dom.tablePanel.dataset.table = tabName;
    const row = element("tr");
    tableDefinitions[tabName].forEach(function (label) {
      const header = element("th", "", label);
      header.scope = "col";
      row.appendChild(header);
    });
    dom.tableHead.replaceChildren(row);
    dom.tableCaption.textContent = tabName === "sessions" ? "会话采集数据" : "消息采集数据";
  }

  function renderRows(tabName, tab) {
    const focusState = captureTableFocus();
    const fragment = document.createDocumentFragment();
    tab.items.forEach(function (item, index) {
      const key = itemKey(tabName, item);
      const expanded = tab.expandedKey === key;
      const detailId = "detail-" + tabName + "-" + index;
      const row = tabName === "sessions"
        ? buildSessionRow(item, key, detailId, expanded)
        : buildMessageRow(item, key, detailId, expanded);
      fragment.appendChild(row);
      if (expanded) {
        fragment.appendChild(buildDetailRow(tabName, item, key, detailId));
      }
    });
    dom.tableBody.replaceChildren(fragment);
    restoreTableFocus(focusState);
  }

  function captureTableFocus() {
    const active = document.activeElement;
    if (!active || !dom.tableBody.contains(active)) {
      return null;
    }
    const detailId = active.getAttribute("aria-controls");
    if (!detailId) {
      return null;
    }
    if (active.classList.contains("expand-button")) {
      return { detailId: detailId, trigger: "expand" };
    }
    if (active.classList.contains("cell-title")) {
      return { detailId: detailId, trigger: "title" };
    }
    return null;
  }

  function restoreTableFocus(focusState) {
    if (!focusState) {
      return;
    }
    const candidates = dom.tableBody.querySelectorAll("button[aria-controls]");
    for (let index = 0; index < candidates.length; index++) {
      const candidate = candidates[index];
      const sameTrigger = focusState.trigger === "expand"
        ? candidate.classList.contains("expand-button")
        : candidate.classList.contains("cell-title");
      if (sameTrigger && candidate.getAttribute("aria-controls") === focusState.detailId) {
        candidate.focus({ preventScroll: true });
        return;
      }
    }
  }

  function buildSessionRow(item, key, detailId, expanded) {
    const row = element("tr", "data-row" + (expanded ? " is-expanded" : ""));
    row.appendChild(wrapCell(sourceBadge(item.sourceType)));

    const sessionCell = element("td", "cell-main");
    const titleButton = element("button", "cell-title", displayValue(item.title, "未命名会话"));
    titleButton.type = "button";
    titleButton.title = displayValue(item.title, "未命名会话");
    titleButton.setAttribute("aria-expanded", String(expanded));
    titleButton.setAttribute("aria-controls", detailId);
    titleButton.addEventListener("click", function () { toggleDetail("sessions", key); });
    const subtitle = element("span", "cell-subtitle", shortId(item.sessionId) + (item.archived === true ? " · 已归档" : ""));
    subtitle.title = displayValue(item.sessionId);
    sessionCell.append(titleButton, subtitle);
    row.appendChild(sessionCell);

    const cwd = element("span", "path-cell", displayValue(item.cwd));
    cwd.title = displayValue(item.cwd);
    row.appendChild(wrapCell(cwd));
    row.appendChild(timeCell(item.lastMessageAt || item.updatedAt));
    row.appendChild(wrapCell(statusBadge(sessionStatus(item))));
    row.appendChild(textCell(commitLabel(item)));
    row.appendChild(expandCell("sessions", key, detailId, expanded));
    return row;
  }

  function buildMessageRow(item, key, detailId, expanded) {
    const row = element("tr", "data-row" + (expanded ? " is-expanded" : ""));
    row.appendChild(textCell("#" + displayValue(item.sequenceNo, "—")));
    row.appendChild(wrapCell(sourceBadge(item.sourceType)));
    row.appendChild(wrapCell(roleBadge(item.role)));

    const contentCell = element("td", "cell-main");
    const previewButton = element("button", "cell-title preview-cell", displayValue(item.contentPreview, "（无文本内容）"));
    previewButton.type = "button";
    previewButton.title = displayValue(item.contentPreview, "无文本内容");
    previewButton.setAttribute("aria-expanded", String(expanded));
    previewButton.setAttribute("aria-controls", detailId);
    previewButton.addEventListener("click", function () { toggleDetail("messages", key, item); });
    const subtitle = element("span", "cell-subtitle", "会话 " + shortId(item.sessionId));
    subtitle.title = displayValue(item.sessionId);
    contentCell.append(previewButton, subtitle);
    row.appendChild(contentCell);

    row.appendChild(wrapCell(eventBadge(item.eventType)));
    row.appendChild(timeCell(item.createdAt || item.ingestedAt));
    row.appendChild(wrapCell(statusBadge(messageStatus(item))));
    row.appendChild(expandCell("messages", key, detailId, expanded, item));
    return row;
  }

  function wrapCell(child) {
    const cell = element("td");
    cell.appendChild(child);
    return cell;
  }

  function textCell(value) {
    return element("td", "", displayValue(value));
  }

  function timeCell(value) {
    const cell = element("td", "time-cell");
    const time = element("time");
    setTime(time, value, true);
    cell.appendChild(time);
    return cell;
  }

  function sourceBadge(value) {
    const source = displayValue(value, "unknown").toLowerCase();
    const known = source === "codex" || source === "claude";
    const badge = element("span", "source-badge " + (known ? "is-" + source : "is-unknown"), known ? source : displayValue(value, "其他"));
    return badge;
  }

  function roleBadge(value) {
    const role = displayValue(value, "unknown").toLowerCase();
    const labels = {
      user: "用户",
      assistant: "模型",
      system: "系统",
      tool: "工具",
      developer: "开发者"
    };
    const className = role === "user" || role === "assistant" ? " is-" + role : "";
    return element("span", "role-badge" + className, labels[role] || displayValue(value, "其他"));
  }

  function eventBadge(value) {
    return element("span", "event-badge", displayValue(value, "message"));
  }

  function statusBadge(status) {
    const badge = element("span", "status-badge is-" + status.kind, status.label);
    return badge;
  }

  function sessionStatus(item) {
    const explicit = explicitStorageStatus(item.storageStatus);
    if (explicit) {
      return explicit;
    }
    if (hasValue(item.pendingCommitId)) {
      return { kind: "pending", label: "待上传" };
    }
    if (hasValue(item.lastCommitId) || hasValue(item.ingestedAt)) {
      return { kind: "uploaded", label: "已上传" };
    }
    return { kind: "local", label: "已采集" };
  }

  function messageStatus(item) {
    const explicit = explicitStorageStatus(item.storageStatus);
    if (explicit) {
      return explicit;
    }
    return hasValue(item.ingestedAt)
      ? { kind: "uploaded", label: "已上传" }
      : { kind: "pending", label: "待上传" };
  }

  function explicitStorageStatus(value) {
    const status = displayValue(value, "").trim().toLowerCase();
    if (status === "pending") {
      return { kind: "pending", label: "待上传" };
    }
    if (status === "uploaded") {
      return { kind: "uploaded", label: "已上传" };
    }
    if (status === "local" || status === "collected") {
      return { kind: "local", label: "已采集" };
    }
    return null;
  }

  function commitLabel(item) {
    if (hasValue(item.pendingCommitId)) {
      return "待提交 #" + item.pendingCommitId;
    }
    if (hasValue(item.lastCommitId)) {
      return "#" + item.lastCommitId;
    }
    return "—";
  }

  function expandCell(tabName, key, detailId, expanded, item) {
    const cell = element("td");
    const button = element("button", "expand-button", "›");
    button.type = "button";
    button.setAttribute("aria-label", expanded ? "收起详情" : "展开详情");
    button.setAttribute("aria-expanded", String(expanded));
    button.setAttribute("aria-controls", detailId);
    button.addEventListener("click", function () { toggleDetail(tabName, key, item); });
    cell.appendChild(button);
    return cell;
  }

  function itemKey(tabName, item) {
    if (tabName === "sessions") {
      return displayValue(item.sourceType, "") + "\u0000" + displayValue(item.sessionId, "");
    }
    return [item.sourceType, item.sessionId, item.messageId, item.sequenceNo].map(function (part) {
      return displayValue(part, "");
    }).join("\u0000");
  }

  function toggleDetail(tabName, key, item) {
    const tab = state.tabs[tabName];
    tab.expandedKey = tab.expandedKey === key ? null : key;
    renderRows(tabName, tab);
    if (tabName === "messages" && tab.expandedKey === key) {
      loadMessageDetail(item, key);
    }
  }

  function buildDetailRow(tabName, item, key, detailId) {
    const row = element("tr", "detail-row");
    row.id = detailId;
    const cell = element("td");
    cell.colSpan = tableDefinitions[tabName].length;
    if (tabName === "sessions") {
      cell.appendChild(buildSessionDetail(item));
    } else {
      const cached = state.detailCache.get(key);
      if (!cached || cached.status === "loading") {
        const loading = element("div", "detail-loader");
        loading.append(element("span", "loader"), element("span", "正在读取消息正文与附件…"));
        cell.appendChild(loading);
      } else if (cached.status === "error") {
        const error = element("div", "detail-error", cached.error);
        cell.appendChild(error);
      } else {
        cell.appendChild(buildMessageDetail(cached.data));
      }
    }
    row.appendChild(cell);
    return row;
  }

  function buildSessionDetail(item) {
    const panel = element("section", "detail-panel");
    const titleRow = element("div", "detail-title-row");
    const titleCopy = element("div");
    titleCopy.append(element("h3", "", displayValue(item.title, "未命名会话")), element("p", "", "会话元信息与上传版本"));
    const viewMessages = element("button", "button button-quiet", "查看此会话消息");
    viewMessages.type = "button";
    viewMessages.addEventListener("click", function () {
      const messages = state.tabs.messages;
      clearTimeout(state.searchTimer);
      messages.sessionId = displayValue(item.sessionId, "");
      messages.sourceType = displayValue(item.sourceType, "");
      messages.query = "";
      messages.role = "";
      messages.eventType = "";
      messages.page = 1;
      selectTab("messages", true);
    });
    titleRow.append(titleCopy, viewMessages);
    panel.append(titleRow, detailGrid([
      ["Session ID", item.sessionId, true],
      ["来源", item.sourceType],
      ["归档状态", item.archived === true ? "已归档" : "活跃"],
      ["工作目录", item.cwd, true],
      ["源文件", item.sourcePath, true],
      ["创建时间", formatDate(item.createdAt, false)],
      ["更新时间", formatDate(item.updatedAt, false)],
      ["采集时间", formatDate(item.ingestedAt, false)],
      ["已上传 Commit", hasValue(item.lastCommitId) ? item.lastCommitId : "—"],
      ["待提交 Commit", hasValue(item.pendingCommitId) ? item.pendingCommitId : "—"]
    ]));
    const jsonSections = element("div", "json-sections is-single");
    jsonSections.appendChild(jsonBlock("完整会话记录", item));
    panel.appendChild(jsonSections);
    return panel;
  }

  async function loadMessageDetail(item, key) {
    const cached = state.detailCache.get(key);
    if (cached && (cached.status === "loading" || cached.status === "ready")) {
      return;
    }
    state.detailCache.set(key, { status: "loading" });
    renderRows("messages", state.tabs.messages);
    const params = new URLSearchParams();
    params.set("sourceType", displayValue(item.sourceType, ""));
    params.set("sessionId", displayValue(item.sessionId, ""));
    params.set("messageId", displayValue(item.messageId, ""));
    params.set("sequenceNo", displayValue(item.sequenceNo, ""));
    try {
      const payload = await fetchJson(API_ROOT + "/messages/detail?" + params.toString());
      state.detailCache.set(key, { status: "ready", data: payload });
    } catch (error) {
      state.detailCache.set(key, { status: "error", error: friendlyError(error) });
    }
    if (state.activeTab === "messages" && state.tabs.messages.expandedKey === key) {
      renderRows("messages", state.tabs.messages);
    }
  }

  function buildMessageDetail(item) {
    const panel = element("section", "detail-panel");
    const attachments = Array.isArray(item.attachments) ? item.attachments : [];
    const contentTruncated = item.contentTruncated === true;
    const titleRow = element("div", "detail-title-row");
    const titleCopy = element("div");
    titleCopy.append(
      element("h3", "", "消息 #" + displayValue(item.sequenceNo, "—")),
      element(
        "p",
        "",
        attachments.length + " 个附件 · " + (contentTruncated ? "正文过长，当前内容已截断" : "完整正文已加载")
      )
    );
    titleRow.appendChild(titleCopy);
    panel.append(titleRow, detailGrid([
      ["Message ID", item.messageId, true],
      ["Session ID", item.sessionId, true],
      ["来源", item.sourceType],
      ["角色", item.role],
      ["事件类型", item.eventType],
      ["序号", item.sequenceNo],
      ["创建时间", formatDate(item.createdAt, false), true],
      ["采集时间", formatDate(item.ingestedAt, false), true]
    ]));

    if (attachments.length > 0) {
      panel.appendChild(buildAttachments(attachments));
    }

    if (contentTruncated) {
      const notice = element("p", "content-truncated-notice", "content_json 超过页面展示上限，以下仅显示服务端返回的前一部分内容。");
      notice.setAttribute("role", "status");
      panel.appendChild(notice);
    }

    const jsonSections = element("div", "json-sections");
    jsonSections.append(
      jsonBlock(contentTruncated ? "content_json（已截断）" : "content_json", item.contentJson),
      jsonBlock("消息详情记录", item)
    );
    panel.appendChild(jsonSections);
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
      visual.addEventListener("click", function () { openAttachmentPreview(attachment, fileName, mimeType, visual); });
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
      card.appendChild(element("span", "unavailable-note", "未提供预览地址"));
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
    state.previewController = controller;
    try {
      const response = await sameOriginFetch(attachment.previewUrl, {
        method: "GET",
        headers: { Accept: mimeType },
        signal: controller.signal
      });
      if (!response.ok) {
        const error = new Error(response.status === 403
          ? "附件请求被拒绝，请确认正在通过本机地址访问"
          : "附件读取失败（HTTP " + response.status + "）");
        error.status = response.status;
        throw error;
      }
      const responseType = (response.headers.get("Content-Type") || mimeType).split(";", 1)[0].trim().toLowerCase();
      if (!ALLOWED_IMAGE_TYPES.has(responseType)) {
        throw new Error("该附件不是支持预览的图片格式");
      }
      const blob = await response.blob();
      if (controller.signal.aborted || dom.previewModal.hidden) {
        return;
      }
      state.previewObjectUrl = URL.createObjectURL(blob);
      dom.previewImage.src = state.previewObjectUrl;
      dom.previewImage.hidden = false;
      dom.previewLoading.hidden = true;
    } catch (error) {
      if (error.name === "AbortError") {
        return;
      }
      if (!dom.previewModal.hidden) {
        dom.previewLoading.classList.add("is-error");
        dom.previewLoading.replaceChildren(element("span", "error-mark", "!"), element("strong", "", friendlyError(error)));
        dom.previewLoading.hidden = false;
      }
    }
  }

  function closePreviewResources() {
    if (state.previewController) {
      state.previewController.abort();
      state.previewController = null;
    }
    if (state.previewObjectUrl) {
      URL.revokeObjectURL(state.previewObjectUrl);
      state.previewObjectUrl = null;
    }
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

  function syncControls(tabName, tab) {
    document.querySelectorAll(".tab").forEach(function (button) {
      const active = button.dataset.tab === tabName;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-selected", String(active));
      button.tabIndex = active ? 0 : -1;
    });
    dom.tablePanel.setAttribute("aria-labelledby", tabName + "-tab");
    dom.searchInput.value = tab.query;
    dom.searchInput.placeholder = tabName === "sessions"
      ? "搜索标题、会话 ID 或工作目录…"
      : "搜索消息内容、消息 ID 或会话 ID…";
    dom.sourceFilter.value = tab.sourceType;
    dom.pageSize.value = String(tab.pageSize);
    const showSessionFilter = tabName === "messages" && Boolean(tab.sessionId);
    dom.sessionFilterChip.hidden = !showSessionFilter;
    dom.sessionFilterValue.textContent = showSessionFilter ? shortId(tab.sessionId) : "";
    dom.sessionFilterValue.title = showSessionFilter ? tab.sessionId : "";
  }

  function configurePagination(defaultValue, maximumValue) {
    const defaultPageSize = positiveWholeNumber(defaultValue, state.pagination.defaultPageSize);
    const maxPageSize = Math.max(
      defaultPageSize,
      positiveWholeNumber(maximumValue, defaultPageSize)
    );
    const firstConfiguration = !state.pagination.configured;
    state.pagination.configured = true;
    state.pagination.defaultPageSize = defaultPageSize;
    state.pagination.maxPageSize = maxPageSize;

    Object.keys(state.tabs).forEach(function (tabName) {
      const tab = state.tabs[tabName];
      if (firstConfiguration) {
        tab.pageSize = defaultPageSize;
      } else if (tab.pageSize > maxPageSize) {
        tab.pageSize = maxPageSize;
        tab.page = 1;
      }
    });
    renderPageSizeOptions();
  }

  function positiveWholeNumber(value, fallback) {
    const number = Number(value);
    return Number.isFinite(number) && number > 0 ? Math.floor(number) : fallback;
  }

  function renderPageSizeOptions() {
    const maximum = state.pagination.maxPageSize;
    const values = new Set([
      state.pagination.defaultPageSize,
      maximum,
      state.tabs.sessions.pageSize,
      state.tabs.messages.pageSize
    ]);
    [10, 25, 50, 100].forEach(function (value) {
      if (value <= maximum) {
        values.add(value);
      }
    });
    const fragment = document.createDocumentFragment();
    Array.from(values)
      .filter(function (value) { return value > 0 && value <= maximum; })
      .sort(function (left, right) { return left - right; })
      .forEach(function (value) {
        const option = element("option", "", value);
        option.value = String(value);
        fragment.appendChild(option);
      });
    dom.pageSize.replaceChildren(fragment);
    dom.pageSize.disabled = false;
    dom.pageSize.value = String(state.tabs[state.activeTab].pageSize);
  }

  function renderTableStates(tabName, tab) {
    dom.loadingState.hidden = !tab.loading;
    dom.errorState.hidden = tab.loading || !tab.error;
    dom.emptyState.hidden = tab.loading || Boolean(tab.error) || !tab.loaded || tab.items.length > 0;
    dom.tableErrorMessage.textContent = tab.error || "";
    const suffix = tab.truncated ? "+" : "";
    dom.recordCount.textContent = formatCount(tab.total) + suffix + " 条记录";
    if (tab.loading) {
      dom.tableStatus.textContent = "正在读取" + (tabName === "sessions" ? "会话" : "消息") + "数据";
    } else if (tab.error) {
      dom.tableStatus.textContent = "数据读取失败";
    } else if (tab.loaded) {
      dom.tableStatus.textContent = "已显示 " + tab.items.length + " 条记录";
    } else {
      dom.tableStatus.textContent = "尚未读取数据";
    }
  }

  function renderPagination(tab) {
    const show = tab.loaded && !tab.loading && !tab.error && tab.total > 0;
    dom.pagination.hidden = !show;
    if (!show) {
      dom.pageNumbers.replaceChildren();
      return;
    }
    const start = (tab.page - 1) * tab.pageSize + 1;
    const end = Math.min(start + tab.items.length - 1, tab.total);
    dom.paginationSummary.textContent = "第 " + formatCount(start) + "–" + formatCount(Math.max(start, end)) + " 条，共 " + formatCount(tab.total) + " 条" + (tab.truncated ? "（结果已截断）" : "");

    const totalPages = Math.max(tab.page, Math.ceil(tab.total / tab.pageSize), tab.hasMore ? tab.page + 1 : 1);
    dom.previousPage.disabled = tab.page <= 1;
    dom.nextPage.disabled = !tab.hasMore;
    const pageItems = visiblePages(totalPages, tab.page);
    const fragment = document.createDocumentFragment();
    pageItems.forEach(function (page) {
      if (page === null) {
        fragment.appendChild(element("span", "page-ellipsis", "…"));
        return;
      }
      const button = element("button", page === tab.page ? "is-current" : "", String(page));
      button.type = "button";
      if (page === tab.page) {
        button.setAttribute("aria-current", "page");
      }
      button.setAttribute("aria-label", "第 " + page + " 页");
      button.addEventListener("click", function () { goToPage(page); });
      fragment.appendChild(button);
    });
    dom.pageNumbers.replaceChildren(fragment);
  }

  function visiblePages(totalPages, current) {
    const pages = new Set([1, totalPages, current - 1, current, current + 1]);
    if (current <= 3) {
      pages.add(2);
      pages.add(3);
    }
    if (current >= totalPages - 2) {
      pages.add(totalPages - 1);
      pages.add(totalPages - 2);
    }
    const sorted = Array.from(pages).filter(function (page) {
      return page >= 1 && page <= totalPages;
    }).sort(function (a, b) { return a - b; });
    const result = [];
    sorted.forEach(function (page, index) {
      if (index > 0 && page - sorted[index - 1] > 1) {
        result.push(null);
      }
      result.push(page);
    });
    return result;
  }

  function goToPage(page) {
    const tab = state.tabs[state.activeTab];
    if (page < 1 || page === tab.page) {
      return;
    }
    tab.page = page;
    loadTable();
    dom.tablePanel.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  function selectTab(tabName, load) {
    if (!state.tabs[tabName]) {
      return;
    }
    state.activeTab = tabName;
    clearTimeout(state.searchTimer);
    renderTable();
    if (load || !state.tabs[tabName].loaded) {
      loadTable();
    }
  }

  async function refreshAll() {
    dom.refreshAll.disabled = true;
    dom.refreshAll.classList.add("is-refreshing");
    state.detailCache.clear();
    const results = await Promise.all([loadOverview(), loadTable({ clearDetails: true })]);
    dom.refreshAll.disabled = false;
    dom.refreshAll.classList.remove("is-refreshing");
    showToast(results.some(Boolean) ? "数据已刷新" : "刷新失败，请稍后重试", !results.some(Boolean));
  }

  async function refreshCurrentTable() {
    dom.refreshTable.disabled = true;
    dom.refreshTable.classList.add("is-refreshing");
    const success = await loadTable({ clearDetails: true });
    dom.refreshTable.disabled = false;
    dom.refreshTable.classList.remove("is-refreshing");
    showToast(success ? "当前表已刷新" : "刷新失败，请稍后重试", !success);
  }

  function applySearch() {
    const tab = state.tabs[state.activeTab];
    const query = dom.searchInput.value;
    if (tab.query === query) {
      return;
    }
    tab.query = query;
    tab.page = 1;
    loadTable();
  }

  function clearFilters() {
    const tab = state.tabs[state.activeTab];
    clearTimeout(state.searchTimer);
    tab.query = "";
    tab.sourceType = "";
    tab.page = 1;
    if (state.activeTab === "messages") {
      tab.sessionId = "";
      tab.role = "";
      tab.eventType = "";
    }
    renderTable();
    loadTable();
  }

  function showToast(message, isError) {
    const toast = element("div", "toast" + (isError ? " is-error" : ""), message);
    dom.toastRegion.appendChild(toast);
    window.setTimeout(function () {
      toast.remove();
    }, 3200);
  }

  function wireEvents() {
    document.querySelectorAll(".tab").forEach(function (button) {
      button.addEventListener("click", function () { selectTab(button.dataset.tab, false); });
      button.addEventListener("keydown", function (event) {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") {
          return;
        }
        event.preventDefault();
        const nextTab = button.dataset.tab === "sessions" ? "messages" : "sessions";
        selectTab(nextTab, false);
        byId(nextTab + "-tab").focus();
      });
    });

    dom.refreshAll.addEventListener("click", refreshAll);
    dom.refreshTable.addEventListener("click", refreshCurrentTable);
    byId("retry-table").addEventListener("click", refreshCurrentTable);
    byId("clear-filters").addEventListener("click", clearFilters);
    byId("clear-session-filter").addEventListener("click", function () {
      state.tabs.messages.sessionId = "";
      state.tabs.messages.page = 1;
      renderTable();
      loadTable();
    });

    dom.searchInput.addEventListener("input", function () {
      clearTimeout(state.searchTimer);
      state.searchTimer = window.setTimeout(applySearch, 350);
    });
    dom.searchInput.addEventListener("keydown", function (event) {
      if (event.isComposing) {
        return;
      }
      if (event.key === "Enter") {
        clearTimeout(state.searchTimer);
        applySearch();
      } else if (event.key === "Escape" && dom.searchInput.value) {
        dom.searchInput.value = "";
        clearTimeout(state.searchTimer);
        applySearch();
      }
    });
    dom.sourceFilter.addEventListener("change", function () {
      const tab = state.tabs[state.activeTab];
      tab.sourceType = dom.sourceFilter.value;
      tab.page = 1;
      loadTable();
    });
    dom.pageSize.addEventListener("change", function () {
      const tab = state.tabs[state.activeTab];
      tab.pageSize = Math.min(
        state.pagination.maxPageSize,
        positiveWholeNumber(dom.pageSize.value, state.pagination.defaultPageSize)
      );
      tab.page = 1;
      loadTable();
    });
    dom.previousPage.addEventListener("click", function () {
      goToPage(state.tabs[state.activeTab].page - 1);
    });
    dom.nextPage.addEventListener("click", function () {
      const tab = state.tabs[state.activeTab];
      if (tab.hasMore) {
        goToPage(tab.page + 1);
      }
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
        dom.searchInput.focus();
      }
    });

    dom.closePreview.addEventListener("click", closePreview);
    dom.previewModal.querySelector("[data-close-preview]").addEventListener("click", closePreview);
    dom.previewImage.addEventListener("error", function () {
      if (!dom.previewModal.hidden) {
        closePreviewResources();
        dom.previewImage.hidden = true;
        dom.previewLoading.classList.add("is-error");
        dom.previewLoading.replaceChildren(element("span", "error-mark", "!"), element("strong", "", "图片解码失败"));
        dom.previewLoading.hidden = false;
      }
    });
    window.addEventListener("beforeunload", closePreviewResources);
  }
})();
