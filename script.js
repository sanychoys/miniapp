(() => {
  const loader = document.getElementById("pageLoader");
  if (!loader) {
    return;
  }

  const minDurationMs = 1500;
  const startedAt = performance.now();
  let isWindowLoaded = document.readyState === "complete";

  const hideLoader = () => {
    if (!isWindowLoaded) {
      return;
    }

    const elapsed = performance.now() - startedAt;
    const remaining = Math.max(0, minDurationMs - elapsed);

    window.setTimeout(() => {
      loader.classList.add("is-hidden");
      loader.addEventListener(
        "transitionend",
        () => {
          loader.remove();
        },
        { once: true }
      );
    }, remaining);
  };

  if (!isWindowLoaded) {
    window.addEventListener(
      "load",
      () => {
        isWindowLoaded = true;
        hideLoader();
      },
      { once: true }
    );
  } else {
    hideLoader();
  }
})();

(() => {
  const webApp = window.Telegram?.WebApp;
  if (!webApp) {
    return;
  }

  const applyViewportMode = () => {
    try {
      if (!webApp.isExpanded) {
        webApp.expand();
      }
    } catch {
      // ignore
    }

    try {
      if (typeof webApp.requestFullscreen === "function" && webApp.isFullscreen !== true) {
        webApp.requestFullscreen();
      }
    } catch {
      // ignore
    }

    try {
      webApp.setHeaderColor?.("#000000");
      webApp.setBackgroundColor?.("#000000");
    } catch {
      // ignore
    }
  };

  webApp.ready();
  applyViewportMode();
  window.setTimeout(applyViewportMode, 120);
  window.setTimeout(applyViewportMode, 420);

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      applyViewportMode();
    }
  });
})();

(() => {
  const decorNodes = Array.from(document.querySelectorAll(".home-about-card-deco"));
  if (!decorNodes.length) {
    return;
  }

  decorNodes.forEach((node) => {
    const rotation = Math.round((Math.random() * 110 - 55) * 10) / 10;
    const scale = (0.92 + Math.random() * 0.22).toFixed(2);
    node.style.setProperty("--deco-rotation", `${rotation}deg`);
    node.style.setProperty("--deco-scale", scale);
  });
})();

(() => {
  const faqOpenButton = document.getElementById("homeFaqOpenButton");
  const faqModal = document.getElementById("faqModal");
  const faqTitle = document.getElementById("faqModalTitle");
  const faqCloseButton = document.getElementById("faqModalCloseButton");
  const faqContent = document.getElementById("faqModalContent");
  const faqExitButton = document.getElementById("faqModalExitButton");
  if (
    !faqOpenButton ||
    !faqModal ||
    !faqTitle ||
    !faqCloseButton ||
    !faqContent ||
    !faqExitButton
  ) {
    return;
  }

  const faqTexts = {
    ru: {
      title: "FAQ",
      closeAria: "Закрыть FAQ",
      close: "Закрыть",
      items: [
        {
          q: "🛍️ Как оформить заказ?",
          a: "Выберите товар, укажите @username (или нажмите «Себе»), количество и нажмите «Оформить заказ».",
        },
        {
          q: "🎟️ Как работает промокод?",
          a: "Введите промокод до оформления заказа. Если код неверный или неактивен, заказ не отправится.",
        },
        {
          q: "💳 Какие методы оплаты доступны?",
          a: "В магазине показывается актуальный способ оплаты. Перед отправкой заказа проверьте сумму в ₽ и $.",
        },
        {
          q: "🔒 Это безопасно?",
          a: "Мы не запрашиваем KYC и лишние данные. Нужен только корректный Telegram username для выдачи.",
        },
        {
          q: "🛠️ Куда писать, если возникла проблема?",
          a: "Нажмите кнопку поддержки над нижним меню и отправьте ID операции или @username для быстрой проверки.",
        },
      ],
    },
    en: {
      title: "FAQ",
      closeAria: "Close FAQ",
      close: "Close",
      items: [
        {
          q: "🛍️ How do I place an order?",
          a: "Choose a product, enter @username (or tap “Me”), set the amount, then tap “Place order”.",
        },
        {
          q: "🎟️ How does a promo code work?",
          a: "Enter the promo code before placing an order. Invalid or inactive codes block order submission.",
        },
        {
          q: "💳 Which payment method is used?",
          a: "The current payment method is shown in the store. Always check both ₽ and $ totals before sending.",
        },
        {
          q: "🔒 Is it safe?",
          a: "No KYC is required. Only a valid Telegram username is needed to deliver your order.",
        },
        {
          q: "🛠️ Where can I contact support?",
          a: "Use the floating support button above the bottom menu and send your operation ID or @username.",
        },
      ],
    },
  };

  const getFaqLang = () => {
    const langValue = String(document.documentElement.lang || "ru").toLowerCase();
    return langValue.startsWith("en") ? "en" : "ru";
  };

  const renderFaqContent = () => {
    const dict = faqTexts[getFaqLang()] || faqTexts.ru;
    faqTitle.textContent = dict.title;
    faqCloseButton.setAttribute("aria-label", dict.closeAria);
    faqExitButton.textContent = dict.close;

    const fragment = document.createDocumentFragment();
    dict.items.forEach((item) => {
      const row = document.createElement("article");
      row.className = "faq-modal-item";

      const question = document.createElement("h3");
      question.className = "faq-modal-question";
      question.textContent = item.q;

      const answer = document.createElement("p");
      answer.className = "faq-modal-answer";
      answer.textContent = item.a;

      row.append(question, answer);
      fragment.appendChild(row);
    });
    faqContent.replaceChildren(fragment);
  };

  let isFaqOpen = false;

  const openFaqModal = () => {
    if (isFaqOpen) {
      return;
    }
    isFaqOpen = true;
    faqModal.hidden = false;
    faqModal.setAttribute("aria-hidden", "false");
    document.body.classList.add("is-faq-modal-open");
    window.requestAnimationFrame(() => {
      faqModal.classList.add("is-open");
    });
  };

  const closeFaqModal = () => {
    if (!isFaqOpen && faqModal.hidden) {
      return;
    }
    isFaqOpen = false;
    faqModal.classList.remove("is-open");
    faqModal.setAttribute("aria-hidden", "true");
    document.body.classList.remove("is-faq-modal-open");
    window.setTimeout(() => {
      if (!isFaqOpen) {
        faqModal.hidden = true;
      }
    }, 180);
  };

  faqOpenButton.addEventListener("click", (event) => {
    event.preventDefault();
    openFaqModal();
  });

  faqCloseButton.addEventListener("click", (event) => {
    event.preventDefault();
    closeFaqModal();
  });

  faqExitButton.addEventListener("click", (event) => {
    event.preventDefault();
    closeFaqModal();
  });

  faqModal.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }
    if (target.closest("[data-faq-close]")) {
      closeFaqModal();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !faqModal.hidden) {
      closeFaqModal();
    }
  });

  window.addEventListener("starslix:screen-changed", () => {
    if (isFaqOpen) {
      closeFaqModal();
    }
  });

  window.addEventListener("starslix:language-updated", renderFaqContent);
  renderFaqContent();
})();

(() => {
  const supportFabButton = document.getElementById("supportFabButton");
  const supportFabBadge = document.getElementById("supportFabBadge");
  const supportFabImage = supportFabButton?.querySelector(".support-fab-image");
  const dock = document.getElementById("appDock");
  const supportModal = document.getElementById("supportModal");
  const supportModalCloseButton = document.getElementById("supportModalCloseButton");
  const supportModalMeta = document.getElementById("supportModalMeta");
  const supportModalMessages = document.getElementById("supportModalMessages");
  const supportModalCompose = document.getElementById("supportModalCompose");
  const supportReplyInput = document.getElementById("supportReplyInput");
  const supportReplyPhotoInput = document.getElementById("supportReplyPhotoInput");
  const supportReplyPhotoName = document.getElementById("supportReplyPhotoName");
  const supportReplyPhotoClearButton = document.getElementById("supportReplyPhotoClearButton");
  const supportReplySendButton = document.getElementById("supportReplySendButton");
  if (
    !supportFabButton ||
    !dock ||
    !supportModal ||
    !supportModalCloseButton ||
    !supportModalMeta ||
    !supportModalMessages ||
    !supportModalCompose ||
    !supportReplyInput ||
    !supportReplyPhotoInput ||
    !supportReplyPhotoName ||
    !supportReplyPhotoClearButton ||
    !supportReplySendButton
  ) {
    return;
  }

  const apiMeta =
    document.querySelector('meta[name="miniapp-api-base"]') ||
    document.querySelector('meta[name="starslix-api-base"]');
  const apiBaseRaw = (apiMeta?.getAttribute("content") || "").trim().replace(/\/+$/, "");
  const apiBase = window.location.protocol === "https:" && /^http:\/\//i.test(apiBaseRaw) ? "" : apiBaseRaw;
  const buildApiUrl = (path) => (apiBase ? `${apiBase}${path}` : path);
  const getInitData = () => String(window.Telegram?.WebApp?.initData || "").trim();

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const showAlert = (text) => {
    if (window.Telegram?.WebApp?.showAlert) {
      window.Telegram.WebApp.showAlert(text);
      return;
    }
    window.alert(text);
  };

  const parseEventDate = (rawValue) => {
    const parsed = new Date(String(rawValue || ""));
    if (Number.isNaN(parsed.getTime())) {
      return "";
    }
    return new Intl.DateTimeFormat("ru-RU", {
      day: "2-digit",
      month: "2-digit",
      year: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    }).format(parsed);
  };

  const readFileAsDataUrl = (file) =>
    new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ""));
      reader.onerror = () => reject(new Error("Не удалось прочитать файл"));
      reader.readAsDataURL(file);
    });

  const compressImageForUpload = (file, { maxSide = 1600, quality = 0.82 } = {}) =>
    new Promise((resolve) => {
      if (!(file instanceof File) || !String(file.type || "").toLowerCase().startsWith("image/")) {
        resolve(file);
        return;
      }
      const objectUrl = URL.createObjectURL(file);
      const image = new Image();
      image.onload = () => {
        try {
          const width = image.naturalWidth || image.width || 0;
          const height = image.naturalHeight || image.height || 0;
          if (!width || !height) {
            URL.revokeObjectURL(objectUrl);
            resolve(file);
            return;
          }
          const scale = Math.min(1, maxSide / Math.max(width, height));
          const targetWidth = Math.max(1, Math.round(width * scale));
          const targetHeight = Math.max(1, Math.round(height * scale));
          const canvas = document.createElement("canvas");
          canvas.width = targetWidth;
          canvas.height = targetHeight;
          const ctx = canvas.getContext("2d");
          if (!ctx) {
            URL.revokeObjectURL(objectUrl);
            resolve(file);
            return;
          }
          ctx.drawImage(image, 0, 0, targetWidth, targetHeight);
          canvas.toBlob(
            (blob) => {
              URL.revokeObjectURL(objectUrl);
              if (blob) {
                resolve(new File([blob], file.name || "image.jpg", { type: "image/jpeg" }));
                return;
              }
              resolve(file);
            },
            "image/jpeg",
            quality
          );
        } catch {
          URL.revokeObjectURL(objectUrl);
          resolve(file);
        }
      };
      image.onerror = () => {
        URL.revokeObjectURL(objectUrl);
        resolve(file);
      };
      image.src = objectUrl;
    });

  const buildPhotoPayload = async (file) => {
    if (!(file instanceof File)) {
      return { imageBase64: "", imageMime: "" };
    }
    const preparedFile = await compressImageForUpload(file);
    const dataUrl = await readFileAsDataUrl(preparedFile);
    const commaIndex = dataUrl.indexOf(",");
    const imageBase64 = commaIndex >= 0 ? dataUrl.slice(commaIndex + 1) : dataUrl;
    return {
      imageBase64,
      imageMime: String(preparedFile.type || "image/jpeg").toLowerCase(),
    };
  };

  const resolvePhotoUrl = (value) => {
    const raw = String(value || "").trim();
    if (!raw) {
      return "";
    }
    if (/^https?:\/\//i.test(raw)) {
      return raw;
    }
    const normalized = raw.startsWith("/") ? raw : `/${raw}`;
    return buildApiUrl(normalized);
  };

  const supportRequest = async (action, payload = {}) => {
    const initData = getInitData();
    if (!initData) {
      throw new Error("Открой мини-приложение через Telegram");
    }

    const response = await fetch(buildApiUrl("/api/miniapp/support/action"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({
        initData,
        action,
        payload,
      }),
    });

    const rawText = await response.text().catch(() => "");
    let data = {};
    try {
      data = rawText ? JSON.parse(rawText) : {};
    } catch {
      data = {};
    }
    if (!response.ok || !data?.ok) {
      if (response.status === 413) {
        throw new Error("Файл слишком большой, загрузите фото поменьше");
      }
      throw new Error(data?.error || "Не удалось выполнить действие поддержки");
    }
    return data?.data || {};
  };

  let cachedSupportState = {
    hasChat: false,
    chat: null,
    messages: [],
    userUnreadCount: 0,
  };
  let isModalOpen = false;
  let isLoading = false;
  let replyPhotoFile = null;
  let pendingReloadTimer = null;

  let lastScrollY = Math.max(0, window.scrollY || 0);
  let frameId = null;

  const isEnglishLocale = () => String(document.documentElement.lang || "ru").toLowerCase().startsWith("en");

  const applyLocale = () => {
    const label = isEnglishLocale() ? "Support" : "Поддержка";
    supportFabButton.setAttribute("aria-label", label);
    supportFabButton.setAttribute("title", label);
  };

  const bindSupportFabImageFallback = () => {
    if (!(supportFabImage instanceof HTMLImageElement)) {
      supportFabButton.classList.add("is-fallback");
      return;
    }
    const applyFallback = () => {
      supportFabButton.classList.add("is-fallback");
    };
    if (supportFabImage.complete && supportFabImage.naturalWidth <= 0) {
      applyFallback();
    }
    supportFabImage.addEventListener("error", applyFallback, { once: true });
  };

  const updateSupportBadge = () => {
    if (!supportFabBadge) {
      return;
    }
    const messages = Array.isArray(cachedSupportState?.messages) ? cachedSupportState.messages : [];
    const hasAdminMessages = messages.some(
      (item) => String(item?.senderRole || "").trim().toLowerCase() === "admin"
    );
    if (!hasAdminMessages) {
      supportFabBadge.hidden = true;
      supportFabBadge.textContent = "";
      return;
    }
    const count = Number.parseInt(String(cachedSupportState?.userUnreadCount || "0"), 10);
    if (!Number.isFinite(count) || count <= 0) {
      supportFabBadge.hidden = true;
      supportFabBadge.textContent = "";
      return;
    }
    supportFabBadge.hidden = false;
    supportFabBadge.textContent = count > 99 ? "99+" : String(count);
  };

  const clearPhotoSelection = () => {
    replyPhotoFile = null;
    supportReplyPhotoInput.value = "";
    supportReplyPhotoName.textContent = "Без файла";
    supportReplyPhotoClearButton.hidden = true;
  };

  const buildMessageHtml = (item) => {
    const role = String(item?.senderRole || "").trim().toLowerCase();
    const isMine = role !== "admin";
    const rowClass = isMine
      ? "support-modal-message support-modal-message--mine"
      : "support-modal-message support-modal-message--admin";
    const authorText = isMine ? "Вы" : "Поддержка";
    const timeText = parseEventDate(item?.createdAt);
    const textValue = String(item?.text || "").trim();
    const photoUrl = resolvePhotoUrl(item?.photoUrl);
    const photoHtml = photoUrl
      ? `<img class="support-modal-message-photo" src="${escapeHtml(photoUrl)}" alt="Фото сообщения" data-photo-open="${escapeHtml(photoUrl)}">`
      : "";
    const messageTextHtml = textValue
      ? `<div class="support-modal-message-text">${escapeHtml(textValue)}</div>`
      : "";
    return `
      <article class="${rowClass}">
        <div class="support-modal-message-head">
          <span class="support-modal-message-author">${escapeHtml(authorText)}</span>
          <span class="support-modal-message-time">${escapeHtml(timeText || "—")}</span>
        </div>
        ${photoHtml}
        ${messageTextHtml}
      </article>
    `;
  };

  const scrollMessagesToBottom = () => {
    supportModalMessages.scrollTop = supportModalMessages.scrollHeight;
  };

  const renderSupportState = (stickToBottom = false) => {
    const hasChat = Boolean(cachedSupportState?.hasChat && cachedSupportState?.chat);
    const messages = Array.isArray(cachedSupportState?.messages) ? cachedSupportState.messages : [];
    const chat = cachedSupportState?.chat || null;

    supportModalMeta.textContent = hasChat
      ? `Чат #${chat.id} · ${chat.title || "Поддержка"}`
      : "Напишите сообщение, и мы ответим в этом чате.";
    supportModal.classList.toggle("is-compact", !hasChat);
    supportModalCompose.hidden = false;

    if (!messages.length) {
      const emptyText = hasChat ? "Пока нет сообщений." : "Чат поддержки пока пуст.";
      supportModalMessages.innerHTML = `<div class="support-modal-empty">${escapeHtml(emptyText)}</div>`;
      cachedSupportState.userUnreadCount = 0;
    } else {
      supportModalMessages.innerHTML = messages.map((item) => buildMessageHtml(item)).join("");
      if (stickToBottom) {
        scrollMessagesToBottom();
      }
    }

    updateSupportBadge();
  };

  const setLoadingState = (disabled) => {
    isLoading = disabled;
    supportReplySendButton.disabled = disabled;
    supportReplyInput.disabled = disabled;
    supportReplyPhotoInput.disabled = disabled;
  };

  const fetchSupportState = async ({ markRead = false, stickToBottom = false } = {}) => {
    if (isLoading) {
      return;
    }
    setLoadingState(true);
    try {
      const stateData = await supportRequest("support_state");
      cachedSupportState = {
        hasChat: Boolean(stateData?.hasChat),
        chat: stateData?.chat || null,
        messages: Array.isArray(stateData?.messages) ? stateData.messages : [],
        userUnreadCount: Number.parseInt(String(stateData?.userUnreadCount || "0"), 10) || 0,
      };

      if (markRead && cachedSupportState.hasChat && cachedSupportState.userUnreadCount > 0) {
        const markPayload = await supportRequest("support_mark_read", {
          chatId: Number(cachedSupportState?.chat?.id || 0),
        });
        cachedSupportState = {
          hasChat: Boolean(markPayload?.hasChat),
          chat: markPayload?.chat || null,
          messages: Array.isArray(markPayload?.messages) ? markPayload.messages : [],
          userUnreadCount: Number.parseInt(String(markPayload?.userUnreadCount || "0"), 10) || 0,
        };
      }

      renderSupportState(stickToBottom);
    } finally {
      setLoadingState(false);
    }
  };

  const queueSupportStateReload = ({ markRead = false, stickToBottom = false } = {}) => {
    if (pendingReloadTimer) {
      window.clearTimeout(pendingReloadTimer);
      pendingReloadTimer = null;
    }
    pendingReloadTimer = window.setTimeout(() => {
      pendingReloadTimer = null;
      fetchSupportState({ markRead, stickToBottom }).catch(() => {});
    }, 120);
  };

  const openSupportModal = () => {
    if (isModalOpen) {
      return;
    }
    isModalOpen = true;
    supportModalCompose.hidden = false;
    supportModal.hidden = false;
    supportModal.setAttribute("aria-hidden", "false");
    document.body.classList.add("is-support-modal-open");
    window.requestAnimationFrame(() => {
      supportModal.classList.add("is-open");
    });
    queueSupportStateReload({ markRead: true, stickToBottom: true });
  };

  const closeSupportModal = () => {
    if (!isModalOpen && supportModal.hidden) {
      return;
    }
    isModalOpen = false;
    supportModal.classList.remove("is-open");
    supportModal.setAttribute("aria-hidden", "true");
    document.body.classList.remove("is-support-modal-open");
    window.setTimeout(() => {
      if (!isModalOpen) {
        supportModal.hidden = true;
      }
    }, 180);
    queueUpdate();
  };

  const shouldShowSupportButton = () => {
    const activeScreen = String(
      document.querySelector(".app-screen.is-active")?.dataset?.screen || ""
    ).trim();
    if (activeScreen && activeScreen !== "home") {
      return false;
    }
    if (document.body.classList.contains("is-faq-modal-open")) {
      return false;
    }
    if (document.body.classList.contains("is-support-modal-open")) {
      return false;
    }
    if (dock.classList.contains("is-input-hidden")) {
      return false;
    }
    return true;
  };

  const updateSupportButton = () => {
    const scrollY = Math.max(0, window.scrollY || document.documentElement.scrollTop || 0);
    const deltaY = scrollY - lastScrollY;
    const isScrollingUp = deltaY < -2;
    const isScrollingDown = deltaY > 2;
    const isAtPageStart = scrollY <= 12;
    const alreadyVisible = supportFabButton.classList.contains("is-visible");
    const canShow = shouldShowSupportButton();

    let shouldBeVisible = alreadyVisible;
    if (!canShow) {
      shouldBeVisible = false;
    } else if (isAtPageStart || isScrollingUp) {
      shouldBeVisible = true;
    } else if (isScrollingDown) {
      shouldBeVisible = false;
    }
    supportFabButton.classList.toggle("is-visible", shouldBeVisible);
    lastScrollY = scrollY;
  };

  const queueUpdate = () => {
    if (frameId !== null) {
      return;
    }
    frameId = window.requestAnimationFrame(() => {
      frameId = null;
      updateSupportButton();
    });
  };

  const sendSupportMessage = async () => {
    if (isLoading) {
      return;
    }
    const replyText = String(supportReplyInput.value || "").trim();
    if (!replyText && !(replyPhotoFile instanceof File)) {
      showAlert("Введите сообщение или добавьте фото");
      return;
    }

    setLoadingState(true);
    try {
      const imagePayload = await buildPhotoPayload(replyPhotoFile);
      const data = await supportRequest("support_send", {
        username: "",
        text: replyText,
        imageBase64: imagePayload.imageBase64,
        imageMime: imagePayload.imageMime,
      });
      cachedSupportState = {
        hasChat: Boolean(data?.hasChat),
        chat: data?.chat || null,
        messages: Array.isArray(data?.messages) ? data.messages : [],
        userUnreadCount: Number.parseInt(String(data?.userUnreadCount || "0"), 10) || 0,
      };
      supportReplyInput.value = "";
      clearPhotoSelection();
      renderSupportState(true);
    } catch (error) {
      showAlert(error.message || "Не удалось отправить сообщение");
    } finally {
      setLoadingState(false);
    }
  };

  const ensurePhotoViewer = () => {
    let root = document.getElementById("supportPhotoViewer");
    if (root) {
      return root;
    }
    root = document.createElement("div");
    root.id = "supportPhotoViewer";
    root.className = "support-photo-viewer";
    root.hidden = true;
    root.innerHTML = `
      <div class="support-photo-viewer-backdrop" data-close-photo-viewer></div>
      <img class="support-photo-viewer-image" id="supportPhotoViewerImage" alt="Фото">
      <button class="support-photo-viewer-close" type="button" data-close-photo-viewer aria-label="Закрыть">✕</button>
    `;
    document.body.appendChild(root);
    root.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) {
        return;
      }
      if (target.closest("[data-close-photo-viewer]")) {
        root.hidden = true;
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && !root.hidden) {
        root.hidden = true;
      }
    });
    return root;
  };

  const openSupportPhoto = (url) => {
    const safeUrl = String(url || "").trim();
    if (!safeUrl) {
      return;
    }
    const viewer = ensurePhotoViewer();
    const imageNode = viewer.querySelector("#supportPhotoViewerImage");
    if (!(imageNode instanceof HTMLImageElement)) {
      return;
    }
    imageNode.src = safeUrl;
    viewer.hidden = false;
  };

  supportFabButton.addEventListener("click", (event) => {
    event.preventDefault();
    openSupportModal();
  });

  supportModalCloseButton.addEventListener("click", (event) => {
    event.preventDefault();
    closeSupportModal();
  });

  supportModal.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }
    if (target.closest("[data-support-close]")) {
      closeSupportModal();
    }
  });

  supportReplyPhotoInput.addEventListener("change", () => {
    replyPhotoFile = supportReplyPhotoInput.files?.[0] || null;
    supportReplyPhotoName.textContent = replyPhotoFile ? replyPhotoFile.name : "Без файла";
    supportReplyPhotoClearButton.hidden = !(replyPhotoFile instanceof File);
  });

  supportReplyPhotoClearButton.addEventListener("click", (event) => {
    event.preventDefault();
    clearPhotoSelection();
  });

  supportReplySendButton.addEventListener("click", () => {
    sendSupportMessage().catch(() => {});
  });

  supportReplyInput.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" || event.shiftKey) {
      return;
    }
    event.preventDefault();
    sendSupportMessage().catch(() => {});
  });

  supportModalMessages.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }
    const image = target.closest("[data-photo-open]");
    if (!image) {
      return;
    }
    const url = String(image.getAttribute("data-photo-open") || "").trim();
    if (!url) {
      return;
    }
    openSupportPhoto(url);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !supportModal.hidden) {
      closeSupportModal();
    }
  });

  window.addEventListener("scroll", queueUpdate, { passive: true });
  window.addEventListener("resize", queueUpdate, { passive: true });
  window.addEventListener("starslix:screen-changed", () => {
    if (isModalOpen) {
      closeSupportModal();
    }
    queueUpdate();
  });
  window.addEventListener("starslix:language-updated", () => {
    applyLocale();
    queueUpdate();
  });
  window.addEventListener("starslix:support-updated", () => {
    queueSupportStateReload({ markRead: isModalOpen, stickToBottom: isModalOpen });
  });
  document.addEventListener("focusin", queueUpdate);
  document.addEventListener("focusout", () => {
    window.setTimeout(queueUpdate, 80);
  });

  applyLocale();
  bindSupportFabImageFallback();
  fetchSupportState({ markRead: false, stickToBottom: false }).catch(() => {
    updateSupportBadge();
  });
  updateSupportButton();
})();

(() => {
  const panel = document.querySelector(".main-glass-panel");
  const tabs = document.querySelectorAll(".panel-tab");
  const views = document.querySelectorAll(".panel-view");
  if (!panel || !tabs.length || !views.length) {
    return;
  }

  const panelClasses = ["is-stars", "is-premium", "is-ton"];

  const activatePanel = (name) => {
    panel.classList.remove(...panelClasses);
    panel.classList.add(`is-${name}`);

    tabs.forEach((tab) => {
      const isActive = tab.dataset.panel === name;
      tab.classList.toggle("is-active", isActive);
      tab.setAttribute("aria-selected", isActive ? "true" : "false");
    });

    views.forEach((view) => {
      view.classList.toggle("is-active", view.dataset.view === name);
    });
  };

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      activatePanel(tab.dataset.panel);
    });
  });
})();

(() => {
  const dock = document.getElementById("appDock");
  const dockOrb = document.getElementById("appDockOrb");
  const dockButtons = Array.from(document.querySelectorAll(".app-dock-button"));
  const screens = Array.from(document.querySelectorAll(".app-screen"));
  if (!dock || !dockOrb || !dockButtons.length || !screens.length) {
    return;
  }

  let selectedScreen = "home";
  let activePointerId = null;
  let isDragging = false;
  let lastPointerX = 0;
  let pendingPointerX = null;
  let pointerFrameId = null;

  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

  const getDockBounds = () => dock.getBoundingClientRect();

  const buildLabelGlyphs = (labelNode) => {
    if (!(labelNode instanceof HTMLElement)) {
      return;
    }

    const hasGlyphMarkup = Boolean(labelNode.querySelector(".dock-glyph"));
    const rawText = hasGlyphMarkup
      ? labelNode.dataset.rawLabelText || labelNode.textContent || ""
      : labelNode.textContent || "";
    labelNode.dataset.rawLabelText = rawText;
    labelNode.textContent = "";

    Array.from(rawText).forEach((char) => {
      const glyph = document.createElement("span");
      glyph.className = "dock-glyph";
      glyph.style.setProperty("--glyph-scale-x", "1");
      glyph.style.setProperty("--glyph-skew-x", "0deg");
      if (char === " ") {
        glyph.classList.add("dock-glyph--space");
        glyph.textContent = "\u00A0";
      } else {
        glyph.textContent = char;
      }
      labelNode.appendChild(glyph);
    });
  };

  const syncDockLabelGlyphs = () => {
    dock.querySelectorAll(".app-dock-label").forEach((label) => {
      buildLabelGlyphs(label);
    });
  };

  const setGlyphDistortionForLabel = (labelNode, lensRect) => {
    if (!(labelNode instanceof HTMLElement)) {
      return;
    }

    const glyphNodes = labelNode.querySelectorAll(".dock-glyph");
    glyphNodes.forEach((glyphNode) => {
      const glyphRect = glyphNode.getBoundingClientRect();
      const glyphWidth = Math.max(glyphRect.width, 1);
      const overlapLeft = Math.max(glyphRect.left, lensRect.left);
      const overlapRight = Math.min(glyphRect.right, lensRect.right);
      const overlapWidth = Math.max(0, overlapRight - overlapLeft);

      if (overlapWidth <= 0) {
        glyphNode.style.setProperty("--glyph-scale-x", "1");
        glyphNode.style.setProperty("--glyph-skew-x", "0deg");
        return;
      }

      const glyphCenter = glyphRect.left + glyphRect.width / 2;
      const nearestEdgeDistance = Math.min(
        Math.abs(glyphCenter - lensRect.left),
        Math.abs(glyphCenter - lensRect.right)
      );

      const coverage = overlapWidth / glyphWidth;
      const edgeRange = Math.max(6, glyphWidth * 2.6);
      const edgeInfluence = clamp(1 - nearestEdgeDistance / edgeRange, 0, 1);
      const partialCoverage = Math.sin(Math.PI * clamp(coverage, 0, 1));
      const overlapCenter = overlapLeft + overlapWidth / 2;
      const centerOffset = clamp((overlapCenter - glyphCenter) / (glyphWidth / 2), -1, 1);
      const stretch = 1 + partialCoverage * edgeInfluence * 1.18;
      const skewX = centerOffset * partialCoverage * edgeInfluence * 20;

      glyphNode.style.setProperty("--glyph-scale-x", stretch.toFixed(3));
      glyphNode.style.setProperty("--glyph-skew-x", `${skewX.toFixed(3)}deg`);
    });
  };

  const resetGlyphDistortionForButton = (button) => {
    button.querySelectorAll(".dock-glyph").forEach((glyphNode) => {
      glyphNode.style.setProperty("--glyph-scale-x", "1");
      glyphNode.style.setProperty("--glyph-skew-x", "0deg");
    });
  };

  const getButtonCenterX = (button) => {
    const rect = button.getBoundingClientRect();
    return rect.left + rect.width / 2;
  };

  const setOrbPosition = (clientX) => {
    const bounds = getDockBounds();
    const orbRect = dockOrb.getBoundingClientRect();
    const halfOrbWidth = orbRect.width / 2;
    const minX = Math.min(halfOrbWidth, bounds.width / 2);
    const maxX = Math.max(bounds.width - halfOrbWidth, bounds.width / 2);
    const normalized = clamp(clientX - bounds.left, minX, maxX);
    dockOrb.style.setProperty("--orb-x", `${normalized}px`);
    lastPointerX = clientX;
  };

  const setLensClipForNode = (targetNode, lensRect) => {
    if (!(targetNode instanceof Element)) {
      return;
    }

    const targetRect = targetNode.getBoundingClientRect();
    const overlapLeft = Math.max(targetRect.left, lensRect.left);
    const overlapRight = Math.min(targetRect.right, lensRect.right);
    const overlapWidth = Math.max(0, overlapRight - overlapLeft);

    if (overlapWidth <= 0 || targetRect.width <= 0) {
      targetNode.style.setProperty("--lens-clip-start", "0%");
      targetNode.style.setProperty("--lens-clip-end", "0%");
      targetNode.style.setProperty("--lens-edge-left", "0");
      targetNode.style.setProperty("--lens-edge-right", "0");
      return;
    }

    const startPercent = ((overlapLeft - targetRect.left) / targetRect.width) * 100;
    const endPercent = ((overlapRight - targetRect.left) / targetRect.width) * 100;
    targetNode.style.setProperty("--lens-clip-start", `${startPercent.toFixed(3)}%`);
    targetNode.style.setProperty("--lens-clip-end", `${endPercent.toFixed(3)}%`);

    const edgeRange = Math.max(8, targetRect.width * 0.3);
    const leftInside = lensRect.left >= targetRect.left && lensRect.left <= targetRect.right;
    const rightInside = lensRect.right >= targetRect.left && lensRect.right <= targetRect.right;

    let leftPull = 0;
    let rightPull = 0;

    if (leftInside) {
      const leftDistance = Math.abs(lensRect.left - targetRect.left);
      leftPull = Math.max(0, Math.min(1, 1 - leftDistance / edgeRange));
    }

    if (rightInside) {
      const rightDistance = Math.abs(targetRect.right - lensRect.right);
      rightPull = Math.max(0, Math.min(1, 1 - rightDistance / edgeRange));
    }

    targetNode.style.setProperty("--lens-edge-left", leftPull.toFixed(3));
    targetNode.style.setProperty("--lens-edge-right", rightPull.toFixed(3));
  };

  const resetLensClip = (button) => {
    const iconWrap = button.querySelector(".app-dock-icon-wrap");
    const labelWrap = button.querySelector(".app-dock-label-wrap");
    iconWrap?.style.setProperty("--lens-clip-start", "0%");
    iconWrap?.style.setProperty("--lens-clip-end", "0%");
    iconWrap?.style.setProperty("--lens-edge-left", "0");
    iconWrap?.style.setProperty("--lens-edge-right", "0");
    labelWrap?.style.setProperty("--lens-clip-start", "0%");
    labelWrap?.style.setProperty("--lens-clip-end", "0%");
    labelWrap?.style.setProperty("--lens-edge-left", "0");
    labelWrap?.style.setProperty("--lens-edge-right", "0");
  };

  const setButtonHighlightByX = (clientX) => {
    const lensRect = dockOrb.getBoundingClientRect();

    dockButtons.forEach((button) => {
      const buttonRect = button.getBoundingClientRect();
      const centerX = getButtonCenterX(button);
      const distance = Math.abs(centerX - clientX);
      const radius = Math.max(buttonRect.width * 1.2, 84);
      const intensity = Math.max(0, Math.min(1, 1 - distance / radius));
      button.style.setProperty("--dock-lens", intensity.toFixed(3));

      setLensClipForNode(button.querySelector(".app-dock-icon-wrap"), lensRect);
      setLensClipForNode(button.querySelector(".app-dock-label-wrap"), lensRect);
      button.querySelectorAll(".app-dock-label").forEach((labelNode) => {
        setGlyphDistortionForLabel(labelNode, lensRect);
      });
    });
  };

  const applyPointerPosition = (clientX) => {
    setOrbPosition(clientX);
    setButtonHighlightByX(clientX);
  };

  const flushPointerFrame = () => {
    pointerFrameId = null;
    if (!isDragging || pendingPointerX === null) {
      return;
    }
    applyPointerPosition(pendingPointerX);
  };

  const queuePointerFrame = (clientX) => {
    pendingPointerX = clientX;
    if (pointerFrameId !== null) {
      return;
    }
    pointerFrameId = window.requestAnimationFrame(flushPointerFrame);
  };

  const setSelectedButton = (targetScreen) => {
    selectedScreen = targetScreen;
    dockButtons.forEach((button) => {
      const isSelected = button.dataset.target === targetScreen;
      button.classList.toggle("is-selected", isSelected);
      if (isSelected) {
        button.setAttribute("aria-current", "page");
      } else {
        button.removeAttribute("aria-current");
      }
      button.style.setProperty("--dock-lens", "0");
      resetLensClip(button);
      resetGlyphDistortionForButton(button);
    });
  };

  const scrollToPageStart = () => {
    window.scrollTo({ top: 0, left: 0, behavior: "auto" });
    if (document.documentElement) {
      document.documentElement.scrollTop = 0;
    }
    if (document.body) {
      document.body.scrollTop = 0;
    }
  };

  const setActiveScreen = (targetScreen) => {
    const knownScreen = screens.some((screen) => screen.dataset.screen === targetScreen);
    const nextScreen = knownScreen ? targetScreen : "home";

    screens.forEach((screen) => {
      const isActive = screen.dataset.screen === nextScreen;
      screen.classList.toggle("is-active", isActive);
      screen.setAttribute("aria-hidden", isActive ? "false" : "true");
    });

    document.body?.classList.toggle("is-home-screen", nextScreen === "home");

    setSelectedButton(nextScreen);
    scrollToPageStart();
    window.dispatchEvent(
      new CustomEvent("starslix:screen-changed", {
        detail: { screen: nextScreen },
      })
    );
  };

  const findClosestButton = (clientX) => {
    let closestButton = dockButtons[0];
    let smallestDistance = Number.POSITIVE_INFINITY;

    dockButtons.forEach((button) => {
      const distance = Math.abs(getButtonCenterX(button) - clientX);
      if (distance < smallestDistance) {
        smallestDistance = distance;
        closestButton = button;
      }
    });

    return closestButton;
  };

  const alignOrbToSelected = () => {
    const selectedButton =
      dockButtons.find((button) => button.dataset.target === selectedScreen) || dockButtons[0];
    setOrbPosition(getButtonCenterX(selectedButton));
  };

  const finishDragging = () => {
    if (!isDragging) {
      return;
    }

    isDragging = false;
    dock.classList.remove("is-dragging");

    const closestButton = findClosestButton(lastPointerX);
    setActiveScreen(closestButton.dataset.target || "home");
    alignOrbToSelected();

    dockButtons.forEach((button) => {
      button.style.setProperty("--dock-lens", "0");
      resetLensClip(button);
      resetGlyphDistortionForButton(button);
    });

    if (pointerFrameId !== null) {
      window.cancelAnimationFrame(pointerFrameId);
      pointerFrameId = null;
    }
    pendingPointerX = null;
    activePointerId = null;
  };

  dockButtons.forEach((button) => {
    button.addEventListener("click", (event) => {
      if (isDragging) {
        event.preventDefault();
        return;
      }
      setActiveScreen(button.dataset.target || "home");
      alignOrbToSelected();
    });
  });

  dock.addEventListener("pointerdown", (event) => {
    if (!(event.target instanceof Element)) {
      return;
    }
    if (!event.target.closest(".app-dock-button")) {
      return;
    }

    isDragging = true;
    activePointerId = event.pointerId;
    dock.classList.add("is-dragging");
    applyPointerPosition(event.clientX);
    dock.setPointerCapture?.(event.pointerId);
    event.preventDefault();
  });

  dock.addEventListener("pointermove", (event) => {
    if (!isDragging || (activePointerId !== null && event.pointerId !== activePointerId)) {
      return;
    }

    queuePointerFrame(event.clientX);
  });

  dock.addEventListener("pointerup", (event) => {
    if (activePointerId !== null && event.pointerId !== activePointerId) {
      return;
    }
    if (isDragging) {
      if (pointerFrameId !== null) {
        window.cancelAnimationFrame(pointerFrameId);
        pointerFrameId = null;
      }
      pendingPointerX = event.clientX;
      applyPointerPosition(event.clientX);
    }
    finishDragging();
  });

  dock.addEventListener("pointercancel", finishDragging);
  dock.addEventListener("lostpointercapture", finishDragging);

  window.addEventListener("starslix:navigate", (event) => {
    const targetScreen = event?.detail?.screen;
    if (typeof targetScreen !== "string") {
      return;
    }
    setActiveScreen(targetScreen);
    alignOrbToSelected();
  });

  window.addEventListener("starslix:dock-labels-updated", syncDockLabelGlyphs);

  syncDockLabelGlyphs();
  setActiveScreen("home");
  alignOrbToSelected();
})();

(() => {
  const homeReviews = document.getElementById("homeReviews");
  const slider = document.getElementById("homeReviewsSlider");
  const track = document.getElementById("homeReviewsTrack");
  const dots = document.getElementById("homeReviewsDots");
  const emptyState = document.getElementById("homeReviewsEmpty");
  if (!homeReviews || !slider || !track || !dots || !emptyState) {
    return;
  }

  const apiBaseMeta = document.querySelector('meta[name="miniapp-api-base"]');
  const apiBase = (apiBaseMeta?.getAttribute("content") || "").trim().replace(/\/$/, "");
  const buildApiUrl = (path) => (apiBase ? `${apiBase}${path}` : path);

  const copy = {
    ru: {
      loading: "Загружаем отзывы...",
      empty: "Отзывов пока нет",
      error: "Не удалось загрузить отзывы",
      fallbackName: "Пользователь Telegram",
    },
    en: {
      loading: "Loading reviews...",
      empty: "No reviews yet",
      error: "Failed to load reviews",
      fallbackName: "Telegram user",
    },
  };

  const getLang = () => (String(document.documentElement.lang || "").toLowerCase() === "en" ? "en" : "ru");
  const t = (key) => copy[getLang()][key] || copy.ru[key];

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const resolveAvatarUrl = (value) => {
    const raw = String(value || "").trim();
    if (!raw) {
      return "";
    }
    if (/^https?:\/\//i.test(raw)) {
      return raw;
    }
    const normalizedPath = raw.startsWith("/") ? raw : `/${raw}`;
    return buildApiUrl(normalizedPath);
  };

  const getAuthorName = (item) => {
    const firstName = String(item?.firstName || "").trim();
    const lastName = String(item?.lastName || "").trim();
    const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();
    if (fullName) {
      return fullName;
    }
    const username = String(item?.username || "").trim().replace(/^@+/, "");
    if (username) {
      return `@${username}`;
    }
    return t("fallbackName");
  };

  const getFallbackGlyph = (authorName) => {
    const normalized = String(authorName || "").trim().replace(/^@+/, "");
    if (!normalized) {
      return "U";
    }
    return normalized.charAt(0).toUpperCase();
  };

  const createAvatarFallback = (glyph) => {
    const fallbackNode = document.createElement("span");
    fallbackNode.className = "home-reviews-avatar-fallback";
    fallbackNode.textContent = glyph;
    return fallbackNode;
  };

  const renderEmptyState = (text) => {
    track.innerHTML = "";
    track.style.transform = "translateX(0%)";
    setTrackTransition(false);
    dots.innerHTML = "";
    dots.hidden = true;
    slidesCount = 0;
    currentSlide = 0;
    visualSlide = 0;
    pendingLoopJump = null;
    isAnimating = false;
    if (animationGuardTimer) {
      window.clearTimeout(animationGuardTimer);
      animationGuardTimer = null;
    }
    emptyState.hidden = false;
    emptyState.textContent = text;
  };

  let currentSlide = 0;
  let slidesCount = 0;
  let visualSlide = 0;
  let isDragging = false;
  let isAnimating = false;
  let dragStartX = 0;
  let dragCurrentX = 0;
  let dragBaseTranslatePx = 0;
  let dragPointerId = null;
  let pendingLoopJump = null;
  let animationGuardTimer = null;
  const trackTransitionValue = "transform 560ms cubic-bezier(0.22, 0.61, 0.36, 1)";

  const finishSlideAnimation = () => {
    if (pendingLoopJump !== null) {
      const jumpTo = pendingLoopJump;
      pendingLoopJump = null;
      applyVisualSlide(jumpTo, false);
      window.requestAnimationFrame(() => {
        setTrackTransition(true);
      });
    }
    isAnimating = false;
    if (animationGuardTimer) {
      window.clearTimeout(animationGuardTimer);
      animationGuardTimer = null;
    }
  };

  const updateDots = () => {
    dots.querySelectorAll(".home-reviews-dot").forEach((dotNode, index) => {
      const isActive = index === currentSlide;
      dotNode.classList.toggle("is-active", isActive);
      dotNode.setAttribute("aria-current", isActive ? "true" : "false");
    });
  };

  const setTrackTransition = (enabled) => {
    track.style.transition = enabled ? trackTransitionValue : "none";
  };

  const getSliderWidth = () => Math.max(1, slider.getBoundingClientRect().width || 0);

  const applyVisualSlide = (nextVisualSlide, animate = true) => {
    if (!slidesCount) {
      return;
    }
    visualSlide = Math.max(0, Number(nextVisualSlide) || 0);
    const nextTransform = `translate3d(-${visualSlide * 100}%, 0, 0)`;
    const prevTransform = String(track.style.transform || "").trim();
    setTrackTransition(animate);
    track.style.transform = nextTransform;
    isAnimating = animate && prevTransform !== nextTransform;
    if (animationGuardTimer) {
      window.clearTimeout(animationGuardTimer);
      animationGuardTimer = null;
    }
    if (isAnimating) {
      animationGuardTimer = window.setTimeout(() => {
        if (isAnimating) {
          finishSlideAnimation();
        }
      }, 760);
    }
  };

  const normalizeSlideIndex = (value) => {
    if (!slidesCount) {
      return 0;
    }
    const numeric = Number(value) || 0;
    return ((numeric % slidesCount) + slidesCount) % slidesCount;
  };

  const goToSlide = (targetSlide, animate = true) => {
    if (!slidesCount || isAnimating) {
      return;
    }
    const normalizedTarget = normalizeSlideIndex(targetSlide);
    if (normalizedTarget === currentSlide) {
      return;
    }
    currentSlide = normalizedTarget;
    const nextVisualSlide = slidesCount > 1 ? currentSlide + 1 : currentSlide;
    applyVisualSlide(nextVisualSlide, animate);
    updateDots();
  };

  const moveNext = () => {
    if (slidesCount <= 1 || isAnimating) {
      return;
    }
    if (currentSlide >= slidesCount - 1) {
      pendingLoopJump = 1;
      currentSlide = 0;
      updateDots();
      applyVisualSlide(slidesCount + 1, true);
      return;
    }
    currentSlide += 1;
    updateDots();
    applyVisualSlide(currentSlide + 1, true);
  };

  const movePrev = () => {
    if (slidesCount <= 1 || isAnimating) {
      return;
    }
    if (currentSlide <= 0) {
      pendingLoopJump = slidesCount;
      currentSlide = slidesCount - 1;
      updateDots();
      applyVisualSlide(0, true);
      return;
    }
    currentSlide -= 1;
    updateDots();
    applyVisualSlide(currentSlide + 1, true);
  };

  const beginDrag = (clientX, pointerId = null) => {
    if (slidesCount <= 1 || isAnimating) {
      return;
    }
    isDragging = true;
    dragStartX = Number(clientX) || 0;
    dragCurrentX = dragStartX;
    dragBaseTranslatePx = -visualSlide * getSliderWidth();
    dragPointerId = pointerId;
    setTrackTransition(false);
  };

  const updateDrag = (clientX) => {
    if (!isDragging) {
      return;
    }
    dragCurrentX = Number(clientX) || dragCurrentX || dragStartX;
    const deltaX = dragCurrentX - dragStartX;
    const nextPx = dragBaseTranslatePx + deltaX;
    track.style.transform = `translate3d(${nextPx}px, 0, 0)`;
  };

  const endDrag = (clientX) => {
    if (!isDragging) {
      return;
    }
    const safeClientX = Number(clientX);
    const finalClientX = Number.isFinite(safeClientX) ? safeClientX : dragCurrentX || dragStartX;
    const deltaX = finalClientX - dragStartX;
    isDragging = false;
    dragPointerId = null;
    const threshold = Math.max(30, getSliderWidth() * 0.16);
    if (Math.abs(deltaX) < threshold) {
      applyVisualSlide(visualSlide, true);
      return;
    }
    if (deltaX < 0) {
      moveNext();
    } else {
      movePrev();
    }
  };

  const cancelDrag = () => {
    if (!isDragging) {
      return;
    }
    isDragging = false;
    dragPointerId = null;
    applyVisualSlide(visualSlide, true);
  };

  const renderReviews = (items) => {
    if (!Array.isArray(items) || !items.length) {
      renderEmptyState(t("empty"));
      return;
    }

    emptyState.hidden = true;
    const slidesHtml = items
      .map((item) => {
        const authorName = getAuthorName(item);
        const fallbackGlyph = getFallbackGlyph(authorName);
        const avatarUrl = resolveAvatarUrl(item?.avatarUrl);
        const reviewText = String(item?.text || "").trim();
        const avatarHtml = avatarUrl
          ? `<img class="home-reviews-avatar" src="${escapeHtml(avatarUrl)}" alt="${escapeHtml(authorName)}" data-fallback="${escapeHtml(fallbackGlyph)}">`
          : `<span class="home-reviews-avatar-fallback">${escapeHtml(fallbackGlyph)}</span>`;

        return `
          <article class="home-reviews-slide">
            <div class="home-reviews-item">
            ${avatarHtml}
              <div class="home-reviews-body">
                <span class="home-reviews-author">${escapeHtml(authorName)}</span>
                <p class="home-reviews-text">${escapeHtml(reviewText)}</p>
              </div>
            </div>
          </article>
        `;
      })
      .join("");

    slidesCount = items.length;
    if (slidesCount > 1) {
      const splitMarker = "</article>";
      const rawSlides = slidesHtml
        .split(splitMarker)
        .map((chunk) => chunk.trim())
        .filter(Boolean)
        .map((chunk) => `${chunk}${splitMarker}`);
      const firstSlide = rawSlides[0] || "";
      const lastSlide = rawSlides[rawSlides.length - 1] || "";
      track.innerHTML = `${lastSlide}${rawSlides.join("")}${firstSlide}`;
    } else {
      track.innerHTML = slidesHtml;
    }

    track.querySelectorAll(".home-reviews-avatar").forEach((avatarNode) => {
      avatarNode.addEventListener(
        "error",
        () => {
          const fallbackGlyph = String(avatarNode.getAttribute("data-fallback") || "U")
            .trim()
            .charAt(0)
            .toUpperCase();
          avatarNode.replaceWith(createAvatarFallback(fallbackGlyph || "U"));
        },
        { once: true }
      );
    });

    dots.innerHTML = Array.from({ length: slidesCount }, (_, index) => {
      const ariaLabel = getLang() === "en" ? `Review ${index + 1}` : `Отзыв ${index + 1}`;
      return `<button class="home-reviews-dot${index === 0 ? " is-active" : ""}" type="button" data-review-dot="${index}" aria-label="${ariaLabel}"></button>`;
    }).join("");
    dots.hidden = slidesCount <= 1;
    currentSlide = 0;
    pendingLoopJump = null;
    visualSlide = slidesCount > 1 ? 1 : 0;
    applyVisualSlide(visualSlide, false);
    updateDots();
  };

  let loading = false;
  let requestSeq = 0;
  let lastLoadedAt = 0;

  const loadReviews = async ({ force = false } = {}) => {
    if (loading) {
      return;
    }
    if (!force && Date.now() - lastLoadedAt < 6000) {
      return;
    }

    loading = true;
    const currentRequest = ++requestSeq;
    if (!track.children.length) {
      renderEmptyState(t("loading"));
    }

    try {
      const response = await fetch(buildApiUrl("/api/miniapp/reviews?limit=10"), {
        method: "GET",
        cache: "no-store",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload?.ok) {
        throw new Error(String(payload?.error || `HTTP ${response.status}`));
      }
      if (currentRequest !== requestSeq) {
        return;
      }
      const items = Array.isArray(payload?.data?.items) ? payload.data.items : [];
      renderReviews(items);
      lastLoadedAt = Date.now();
    } catch {
      if (currentRequest !== requestSeq) {
        return;
      }
      renderEmptyState(t("error"));
    } finally {
      if (currentRequest === requestSeq) {
        loading = false;
      }
    }
  };

  dots.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const dotButton = target.closest("[data-review-dot]");
    if (!(dotButton instanceof HTMLElement)) {
      return;
    }
    const dotIndex = Number.parseInt(String(dotButton.dataset.reviewDot || ""), 10);
    if (!Number.isFinite(dotIndex)) {
      return;
    }
    goToSlide(dotIndex, true);
  });

  const supportsPointerEvents = typeof window.PointerEvent === "function";
  let ignoreMouseUntil = 0;

  slider.addEventListener("dragstart", (event) => {
    event.preventDefault();
  });

  if (supportsPointerEvents) {
    slider.addEventListener("pointerdown", (event) => {
      if (event.button !== 0) {
        return;
      }
      beginDrag(event.clientX, event.pointerId);
      slider.setPointerCapture?.(event.pointerId);
      event.preventDefault();
    });

    slider.addEventListener("pointermove", (event) => {
      if (dragPointerId !== null && event.pointerId !== dragPointerId) {
        return;
      }
      updateDrag(event.clientX);
      if (isDragging) {
        event.preventDefault();
      }
    });

    slider.addEventListener("pointerup", (event) => {
      if (dragPointerId !== null && event.pointerId !== dragPointerId) {
        return;
      }
      endDrag(event.clientX);
    });

    slider.addEventListener("pointercancel", () => {
      cancelDrag();
    });
  } else {
    slider.addEventListener(
      "touchstart",
      (event) => {
        if (!event.changedTouches || !event.changedTouches.length) {
          return;
        }
        ignoreMouseUntil = Date.now() + 700;
        beginDrag(event.changedTouches[0].clientX, null);
      },
      { passive: true }
    );

    slider.addEventListener(
      "touchmove",
      (event) => {
        if (!event.changedTouches || !event.changedTouches.length) {
          return;
        }
        updateDrag(event.changedTouches[0].clientX);
      },
      { passive: true }
    );

    slider.addEventListener(
      "touchend",
      (event) => {
        if (!event.changedTouches || !event.changedTouches.length) {
          return;
        }
        endDrag(event.changedTouches[0].clientX);
      },
      { passive: true }
    );

    slider.addEventListener(
      "touchcancel",
      () => {
        cancelDrag();
      },
      { passive: true }
    );

    slider.addEventListener("mousedown", (event) => {
      if (event.button !== 0 || Date.now() < ignoreMouseUntil) {
        return;
      }
      beginDrag(event.clientX, null);
    });

    window.addEventListener("mousemove", (event) => {
      updateDrag(event.clientX);
    });

    window.addEventListener("mouseup", (event) => {
      endDrag(event.clientX);
    });
  }

  window.addEventListener("blur", () => {
    cancelDrag();
  });

  track.addEventListener("transitionend", (event) => {
    if (event.propertyName !== "transform") {
      return;
    }
    finishSlideAnimation();
  });

  window.addEventListener("starslix:screen-changed", (event) => {
    const screen = String(event?.detail?.screen || "");
    if (screen === "home") {
      loadReviews({ force: false });
    }
  });

  window.addEventListener("starslix:reviews-updated", () => {
    if (!document.body.classList.contains("is-home-screen")) {
      return;
    }
    loadReviews({ force: true });
  });

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && document.body.classList.contains("is-home-screen")) {
      loadReviews({ force: true });
    }
  });

  loadReviews({ force: false });
})();

(() => {
  const grids = document.querySelectorAll(".offer-grid");
  if (!grids.length) {
    return;
  }

  const defaultUsdRubRate = 76.5;
  const defaultStarRates = {
    "50_75": 1.7,
    "76_100": 1.6,
    "101_250": 1.55,
    "251_plus": 1.5,
  };

  const getMiniappPricingConfig = () => window.starslixMiniappConfig || {};
  const getProfilePerks = () => window.starslixProfilePerks || {};
  const getUsdRubRate = () => {
    const numeric = Number(getMiniappPricingConfig().usdRubRate);
    return Number.isFinite(numeric) && numeric > 0 ? numeric : defaultUsdRubRate;
  };
  const getProfileFixedStarRate = () => {
    const numeric = Number(getProfilePerks().fixedStarRateRub);
    return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
  };
  const getStarRates = () => ({
    ...defaultStarRates,
    ...(getMiniappPricingConfig().starRates || {}),
  });
  const roundToTwo = (value) => Math.round(Number(value) * 100) / 100;

  const getStarsRateForAmount = (starsCount) => {
    const fixedRate = getProfileFixedStarRate();
    if (fixedRate !== null) {
      return fixedRate;
    }
    const rates = getStarRates();
    if (starsCount >= 50 && starsCount <= 75) {
      return Number(rates["50_75"]) || defaultStarRates["50_75"];
    }
    if (starsCount >= 76 && starsCount <= 100) {
      return Number(rates["76_100"]) || defaultStarRates["76_100"];
    }
    if (starsCount >= 101 && starsCount <= 250) {
      return Number(rates["101_250"]) || defaultStarRates["101_250"];
    }
    return Number(rates["251_plus"]) || defaultStarRates["251_plus"];
  };

  const formatRubValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${Number.isInteger(numeric) ? numeric : numeric.toFixed(2)}₽`;
  };

  const formatUsdValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${numeric.toFixed(2)}$`;
  };

  const getPremiumPriceFromConfig = (monthsValue) => {
    const premiumOffers = Array.isArray(getMiniappPricingConfig().premium)
      ? getMiniappPricingConfig().premium
      : [];
    const match = premiumOffers.find((offer) => Number(offer?.months) === Number(monthsValue));
    if (!match) {
      return null;
    }
    return {
      usd: formatUsdValue(match.usd),
      rub: formatRubValue(match.rub),
    };
  };

  grids.forEach((grid) => {
    const cards = Array.from(grid.querySelectorAll(".offer-card"));
    const panelView = grid.closest(".panel-view");
    const quantityInput = panelView?.querySelector('.form-input[type="number"]');
    const priceUsdNode = panelView?.querySelector("[data-price-usd]");
    const priceRubNode = panelView?.querySelector("[data-price-rub]");

    const getCardAmount = (card) =>
      card.querySelector(".offer-card-amount")?.textContent?.trim() || "";

    const getCardPrice = (card) => {
      const rawPrice = card?.querySelector(".offer-card-price")?.textContent?.trim() || "";
      const [usdPart = "", rubPart = ""] = rawPrice.split("/");
      return {
        usd: usdPart.trim(),
        rub: rubPart.trim(),
      };
    };

    const syncPriceRow = (card) => {
      if (!priceUsdNode || !priceRubNode) {
        return;
      }

      const isStarsView = panelView?.dataset?.view === "stars";
      if (isStarsView && quantityInput) {
        const amount = Number.parseInt(String(quantityInput.value || "").trim(), 10);
        if (Number.isFinite(amount) && amount >= 50 && amount <= 10000) {
          const rub = roundToTwo(amount * getStarsRateForAmount(amount));
          const usd = roundToTwo(rub / getUsdRubRate());
          priceUsdNode.textContent = formatUsdValue(usd);
          priceRubNode.textContent = formatRubValue(rub);
          return;
        }
      }

      const sourceCard =
        card || cards.find((item) => item.classList.contains("is-active")) || null;
      if (!sourceCard) {
        priceUsdNode.textContent = "—";
        priceRubNode.textContent = "—";
        return;
      }

      const isPremiumView = panelView?.dataset?.view === "premium";
      if (isPremiumView) {
        const months = Number.parseInt(getCardAmount(sourceCard).replace(/[^0-9]/g, ""), 10);
        if (Number.isFinite(months)) {
          const premiumPrice = getPremiumPriceFromConfig(months);
          if (premiumPrice) {
            priceUsdNode.textContent = premiumPrice.usd;
            priceRubNode.textContent = premiumPrice.rub;
            return;
          }
        }
      }

      const { usd, rub } = getCardPrice(sourceCard);
      priceUsdNode.textContent = usd || "—";
      priceRubNode.textContent = rub || "—";
    };

    const setActiveCard = (activeCard) => {
      cards.forEach((item) => item.classList.remove("is-active"));
      if (activeCard) {
        activeCard.classList.add("is-active");
      }
    };

    const findCardByAmount = (value) => {
      const normalized = String(value).trim();
      if (!normalized) {
        return null;
      }

      return (
        cards.find((card) => getCardAmount(card) === normalized) || null
      );
    };

    const syncInputWithActiveCard = () => {
      const activeCard = cards.find((item) => item.classList.contains("is-active"));
      if (quantityInput) {
        const nextValue = activeCard ? getCardAmount(activeCard) : "";
        if (quantityInput.value !== nextValue) {
          quantityInput.value = nextValue;
        }
      }

      syncPriceRow(activeCard);
    };

    const initialActiveCard = grid.querySelector(".offer-card.is-active");
    if (initialActiveCard) {
      setActiveCard(initialActiveCard);
    }
    syncInputWithActiveCard();

    const refreshGridPrices = () => {
      const activeCard = cards.find((item) => item.classList.contains("is-active")) || null;
      syncPriceRow(activeCard);
    };
    window.addEventListener("starslix:pricing-updated", refreshGridPrices);
    window.addEventListener("starslix:profile-perks-updated", refreshGridPrices);

    cards.forEach((card) => {
      card.addEventListener("click", () => {
        const shouldDeactivate = card.classList.contains("is-active");
        setActiveCard(shouldDeactivate ? null : card);
        syncInputWithActiveCard();
      });
    });

    if (quantityInput) {
      quantityInput.addEventListener("input", () => {
        const matchedCard = findCardByAmount(quantityInput.value);
        setActiveCard(matchedCard);
        syncPriceRow(matchedCard);
      });
    }
  });
})();

(() => {
  const usernameInputs = document.querySelectorAll(".form-input--telegram");
  const selfFillButtons = document.querySelectorAll(".form-self-fill-button");
  if (!usernameInputs.length && !selfFillButtons.length) {
    return;
  }

  const sanitizeTelegramUsername = (rawValue) => {
    const cleaned = rawValue.replace(/[^@A-Za-z0-9_]/g, "");
    const usernamePart = cleaned.replace(/@/g, "").slice(0, 32);

    if (!cleaned) {
      return "";
    }

    return `@${usernamePart}`;
  };

  usernameInputs.forEach((input) => {
    input.addEventListener("input", () => {
      const sanitizedValue = sanitizeTelegramUsername(input.value);
      if (input.value !== sanitizedValue) {
        input.value = sanitizedValue;
      }
    });
  });

  const getStoredProfileUsername = () => {
    try {
      const raw = window.localStorage.getItem("starslix:profile-user");
      const parsed = raw ? JSON.parse(raw) : null;
      if (!parsed || typeof parsed !== "object") {
        return "";
      }
      return String(parsed.username || "").trim();
    } catch {
      return "";
    }
  };

  const getCurrentTelegramUsername = () => {
    const initUsernameRaw = String(
      window.Telegram?.WebApp?.initDataUnsafe?.user?.username || ""
    ).trim();
    const initUsername = sanitizeTelegramUsername(initUsernameRaw);
    if (initUsername) {
      return initUsername;
    }

    const storedUsername = sanitizeTelegramUsername(getStoredProfileUsername());
    if (storedUsername) {
      return storedUsername;
    }

    return "";
  };

  const blurActiveEditable = () => {
    const active = document.activeElement;
    if (!(active instanceof HTMLElement)) {
      return;
    }
    if (
      active instanceof HTMLInputElement ||
      active instanceof HTMLTextAreaElement ||
      active instanceof HTMLSelectElement ||
      active.isContentEditable
    ) {
      active.blur();
    }
  };

  selfFillButtons.forEach((button) => {
    button.addEventListener("pointerdown", () => {
      blurActiveEditable();
    });

    button.addEventListener("click", (event) => {
      event.preventDefault();
      blurActiveEditable();
      const wrap = button.closest(".form-input-wrap");
      const input = wrap?.querySelector(".form-input--telegram");
      if (!(input instanceof HTMLInputElement)) {
        return;
      }

      const usernameValue = getCurrentTelegramUsername();
      if (!usernameValue) {
        return;
      }

      input.value = usernameValue;
      input.dispatchEvent(new Event("input", { bubbles: true }));
      input.dispatchEvent(new Event("change", { bubbles: true }));
      input.blur();
      window.requestAnimationFrame(() => blurActiveEditable());
    });
  });
})();

(() => {
  const dismissKeyboardIfNeeded = (event) => {
    const activeElement = document.activeElement;
    if (!activeElement) {
      return;
    }

    const isTextInput =
      activeElement.tagName === "INPUT" || activeElement.tagName === "TEXTAREA";
    if (!isTextInput) {
      return;
    }

    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }

    if (target.closest(".form-self-fill-button")) {
      activeElement.blur();
      return;
    }

    if (target.closest("input, textarea, select, [contenteditable=''], [contenteditable='true'], label")) {
      return;
    }

    if (target.closest(".form-input-wrap")) {
      return;
    }

    activeElement.blur();
  };

  document.addEventListener("pointerdown", dismissKeyboardIfNeeded);
  document.addEventListener("touchstart", dismissKeyboardIfNeeded, { passive: true });
})();

(() => {
  const dock = document.getElementById("appDock");
  if (!dock) {
    return;
  }

  const isFormInput = (node) =>
    node instanceof HTMLInputElement ||
    node instanceof HTMLTextAreaElement ||
    node instanceof HTMLSelectElement;

  const isInputHiddenType = (node) =>
    node instanceof HTMLInputElement && node.type === "hidden";

  const updateDockVisibilityForInput = () => {
    const activeElement = document.activeElement;
    const shouldHide =
      isFormInput(activeElement) &&
      !activeElement.disabled &&
      !isInputHiddenType(activeElement);

    dock.classList.toggle("is-input-hidden", Boolean(shouldHide));
  };

  document.addEventListener("focusin", updateDockVisibilityForInput);
  document.addEventListener("focusout", () => {
    window.setTimeout(updateDockVisibilityForInput, 0);
  });

  window.addEventListener("starslix:navigate", () => {
    window.setTimeout(updateDockVisibilityForInput, 0);
  });
})();

(() => {
  const avatarToggle = document.getElementById("userAvatarToggle");
  const quickMenu = document.getElementById("headerQuickMenu");
  const storePage = document.getElementById("storePage");
  const profilePage = document.getElementById("profilePage");
  const profileCloseButton = document.getElementById("profileCloseButton");
  const profileAvatar = document.getElementById("profileAvatar");
  const profileFullname = document.getElementById("profileFullname");
  const profileUsername = document.getElementById("profileUsername");
  const profileTelegramId = document.getElementById("profileTelegramId");
  const profileHistoryList = document.getElementById("profileHistoryList");
  const profileHistoryEmpty = document.getElementById("profileHistoryEmpty");
  const profileHistoryControls = document.getElementById("profileHistoryControls");
  const profileHistoryPrev = document.getElementById("profileHistoryPrev");
  const profileHistoryNext = document.getElementById("profileHistoryNext");
  const profileHistoryPage = document.getElementById("profileHistoryPage");
  const profileAdminActions = document.getElementById("profileAdminActions");
  const profileAdminButton = document.getElementById("profileAdminButton");
  const profileAdminStatus = document.getElementById("profileAdminStatus");
  const profileStatPurchasesValue = document.getElementById("profileStatPurchasesValue");
  const profileStatSpentValue = document.getElementById("profileStatSpentValue");
  const profileStatPromosValue = document.getElementById("profileStatPromosValue");
  const profileStatPromosSavings = document.getElementById("profileStatPromosSavings");
  const profileProgressLevel = document.getElementById("profileProgressLevel");
  const profileProgressFill = document.getElementById("profileProgressFill");
  const profileProgressNote = document.getElementById("profileProgressNote");
  const profileHistoryFilters = Array.from(document.querySelectorAll(".profile-history-filter"));
  const profileHistorySearchInput = document.getElementById("profileHistorySearchInput");
  const profileHistorySearchButton = document.getElementById("profileHistorySearchButton");
  const profileHistoryEmptyAction = document.getElementById("profileHistoryEmptyAction");
  const homeAboutStarsValue = document.getElementById("homeAboutStarsValue");
  const homeAboutPremiumValue = document.getElementById("homeAboutPremiumValue");
  const appAvatarNodes = document.querySelectorAll(
    ".user-avatar, #profileAvatar, .app-dock-button[data-target=\"profile\"] .app-dock-icon--avatar"
  );
  if (!storePage || !profilePage) {
    return;
  }

  const historyStorageKey = "starslix_purchase_history";
  const historyPageSize = 8;
  let historyPage = 0;
  let historyFilter = "all";
  let historySearchQuery = "";
  let profileStatsFromApi = null;
  let lastProfilePerksSignature = "";
  const historyTypeMeta = {
    stars: {
      icon: "zvezda.png",
    },
    premium: {
      icon: "blackpremicon.png",
    },
    ton: {
      icon: "blactonicon.png",
    },
  };

  const setProfilePageState = (isProfilePage) => {
    document.body.classList.toggle("is-profile-page", isProfilePage);
    if (avatarToggle) {
      avatarToggle.setAttribute("aria-expanded", isProfilePage ? "true" : "false");
    }
    profilePage.setAttribute("aria-hidden", isProfilePage ? "false" : "true");
    storePage.setAttribute("aria-hidden", isProfilePage ? "true" : "false");
    if (!isProfilePage) {
      closeHistoryOperationModal();
    }
  };

  const formatHistoryDate = (isoValue) => {
    const parsedDate = new Date(isoValue);
    if (Number.isNaN(parsedDate.getTime())) {
      return "";
    }
    const locale = isEnglishLocale() ? "en-US" : "ru-RU";
    return new Intl.DateTimeFormat(locale, {
      day: "2-digit",
      month: "2-digit",
      year: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }).format(parsedDate);
  };

  const isEnglishLocale = () => document.documentElement.lang === "en";

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const formatIntValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "0";
    }
    return new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 }).format(
      Math.max(0, Math.round(numeric))
    );
  };

  const formatRubValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "0₽";
    }
    const hasFraction = Math.abs(numeric - Math.round(numeric)) > 0.001;
    const formatted = new Intl.NumberFormat("ru-RU", {
      minimumFractionDigits: hasFraction ? 2 : 0,
      maximumFractionDigits: 2,
    }).format(numeric);
    return `${formatted}₽`;
  };

  const formatUsdValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "0$";
    }
    const hasFraction = Math.abs(numeric - Math.round(numeric)) > 0.001;
    const formatted = new Intl.NumberFormat("en-US", {
      minimumFractionDigits: hasFraction ? 2 : 0,
      maximumFractionDigits: 2,
    }).format(numeric);
    return `${formatted}$`;
  };

  const getProfileLevelInfoBySpent = (totalSpentRub) => {
    const spent = Math.max(0, Number(totalSpentRub) || 0);
    if (spent > 50000) {
      return {
        level: 4,
        minSpent: 50000,
        nextTarget: null,
        cashbackPercent: 7,
        personalOffers: true,
        fixedStarRateRub: 1.5,
      };
    }
    if (spent >= 20000) {
      return {
        level: 3,
        minSpent: 20000,
        nextTarget: 50000,
        cashbackPercent: 4,
        personalOffers: true,
        fixedStarRateRub: null,
      };
    }
    if (spent >= 5000) {
      return {
        level: 2,
        minSpent: 5000,
        nextTarget: 20000,
        cashbackPercent: 2,
        personalOffers: false,
        fixedStarRateRub: null,
      };
    }
    return {
      level: 1,
      minSpent: 0,
      nextTarget: 5000,
      cashbackPercent: 0,
      personalOffers: false,
      fixedStarRateRub: null,
    };
  };

  const buildProfilePerksText = (levelInfo) => {
    const safeLevel = levelInfo || getProfileLevelInfoBySpent(0);
    if (safeLevel.level <= 1) {
      return isEnglishLocale() ? "Standard price" : "Стандартная цена";
    }
    const parts = [];
    parts.push(
      isEnglishLocale()
        ? `Cashback ${safeLevel.cashbackPercent}% in stars`
        : `Кэшбэк ${safeLevel.cashbackPercent}% в звёздах`
    );
    if (safeLevel.personalOffers) {
      parts.push(isEnglishLocale() ? "Personal offers" : "Персональные предложения");
    }
    const fixedRateValue = Number(safeLevel.fixedStarRateRub);
    if (Number.isFinite(fixedRateValue) && fixedRateValue > 0) {
      parts.push(
        isEnglishLocale()
          ? `Fixed rate ${fixedRateValue.toFixed(1)}₽/⭐`
          : `Фикс. курс ${fixedRateValue.toFixed(1)}₽/⭐`
      );
    }
    return parts.join(" • ");
  };

  const syncProfilePerksToWindow = (levelInfo) => {
    const fixedRateNumeric = Number(levelInfo?.fixedStarRateRub);
    const payload = {
      level: Math.max(1, Number(levelInfo?.level) || 1),
      cashbackPercent: Math.max(0, Number(levelInfo?.cashbackPercent) || 0),
      personalOffers: Boolean(levelInfo?.personalOffers),
      fixedStarRateRub:
        Number.isFinite(fixedRateNumeric) && fixedRateNumeric > 0 ? fixedRateNumeric : null,
    };
    window.starslixProfilePerks = payload;
    const signature = `${payload.level}|${payload.cashbackPercent}|${payload.personalOffers ? 1 : 0}|${
      payload.fixedStarRateRub ?? ""
    }`;
    if (signature === lastProfilePerksSignature) {
      return;
    }
    lastProfilePerksSignature = signature;
    window.dispatchEvent(
      new CustomEvent("starslix:profile-perks-updated", {
        detail: payload,
      })
    );
  };

  const extractRubFromPriceLabel = (priceValue) => {
    const text = String(priceValue || "").replace(/\s+/g, "");
    const rubMatch = text.match(/([0-9]+(?:[.,][0-9]+)?)₽/i);
    if (!rubMatch) {
      return Number.NaN;
    }
    const numeric = Number(rubMatch[1].replace(",", "."));
    return Number.isFinite(numeric) ? numeric : Number.NaN;
  };

  const getHistoryStatusLabel = (status) => {
    const safeStatus = String(status || "").toLowerCase();
    if (isEnglishLocale()) {
      if (safeStatus === "error") {
        return "Error";
      }
      if (safeStatus === "warning") {
        return "In progress";
      }
      if (safeStatus === "pending") {
        return "In progress";
      }
      return "Completed";
    }
    if (safeStatus === "error") {
      return "Ошибка";
    }
    if (safeStatus === "warning") {
      return "В работе";
    }
    if (safeStatus === "pending") {
      return "В работе";
    }
    return "Выполнено";
  };

  const normalizeHistoryStatus = (item) => {
    const raw = String(item?.status || "").trim().toLowerCase();
    if (["success", "ok", "done", "paid", "completed"].includes(raw)) {
      return "success";
    }
    if (["pending", "wait", "processing", "queued"].includes(raw)) {
      return "pending";
    }
    if (["warning", "warn", "check", "promo_warning"].includes(raw)) {
      return "warning";
    }
    if (["error", "failed", "fail", "rejected", "cancelled"].includes(raw)) {
      return "error";
    }
    if (String(item?.promoError || item?.promo_error || "").trim()) {
      return "warning";
    }
    return "success";
  };

  const loadHistory = () => {
    try {
      const rawValue = window.localStorage.getItem(historyStorageKey);
      const parsed = rawValue ? JSON.parse(rawValue) : [];
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  };

  const saveHistory = (items) => {
    try {
      window.localStorage.setItem(historyStorageKey, JSON.stringify(items));
    } catch {
      // ignore write errors
    }
  };

  let historyItems = loadHistory();

  const normalizeHistoryType = (item) => {
    const rawType = String(item?.type || "").trim().toLowerCase();
    if (rawType === "stars" || rawType === "premium" || rawType === "ton") {
      return rawType;
    }

    const hint = String(item?.main || "").trim().toLowerCase();
    if (hint.includes("ton")) {
      return "ton";
    }
    if (hint.includes("прем") || hint.includes("premium") || hint.includes("мес")) {
      return "premium";
    }
    return "stars";
  };

  const normalizeHistoryMain = (item, type) => {
    const rawMain = String(item?.main || "").trim();
    if (!rawMain) {
      return "—";
    }

    if (type === "stars") {
      return rawMain.replace(/^(зв[её]зды?|stars?)\s*[:\-]?\s*/i, "").trim() || "—";
    }
    if (type === "premium") {
      return rawMain.replace(/^(премиум|premium)\s*[:\-]?\s*/i, "").trim() || "—";
    }
    return rawMain.replace(/^(ton)\s*[:\-]?\s*/i, "").trim() || "—";
  };

  const normalizeHistoryEntry = (item) => {
    const type = normalizeHistoryType(item);
    const explicitPriceRub = Number(item?.priceRub);
    const explicitPriceUsd = Number(item?.priceUsd);
    const rawPriceText = String(item?.price || "").trim() || "—";
    const priceText = rawPriceText.replace(/в‚Ѕ/g, "₽");
    const priceRub = Number.isFinite(explicitPriceRub)
      ? explicitPriceRub
      : extractRubFromPriceLabel(priceText);
    const priceUsd = Number.isFinite(explicitPriceUsd) ? explicitPriceUsd : null;
    const promoCode = String(item?.promoCode || item?.promo_code || "")
      .trim()
      .toUpperCase();
    const promoDiscountRaw = Number(item?.promoDiscount ?? item?.promo_discount);
    const promoDiscount = Number.isFinite(promoDiscountRaw) ? promoDiscountRaw : 0;
    const promoError = String(item?.promoError || item?.promo_error || "").trim();
    const operationId = String(item?.operationId || item?.operation_id || "")
      .trim()
      .toUpperCase();
    const operationSourceRaw = String(item?.operationSource || item?.operation_source || "")
      .trim()
      .toLowerCase();
    const operationSource =
      operationSourceRaw === "miniapp" || operationSourceRaw === "legacy"
        ? operationSourceRaw
        : "";
    const operationNumericCandidate = Number.parseInt(
      String(item?.operationNumericId ?? item?.operation_numeric_id ?? item?.historyId ?? ""),
      10
    );
    const operationNumericId =
      Number.isFinite(operationNumericCandidate) && operationNumericCandidate > 0
        ? operationNumericCandidate
        : 0;
    const buyerUserIdCandidate = Number.parseInt(
      String(item?.buyerUserId ?? item?.buyer_user_id ?? item?.userId ?? ""),
      10
    );
    const buyerUserId =
      Number.isFinite(buyerUserIdCandidate) && buyerUserIdCandidate > 0 ? buyerUserIdCandidate : 0;
    const buyerUsername = String(item?.buyerUsername || item?.buyer_username || "")
      .trim()
      .replace(/^@+/, "");
    const buyerFullName = String(item?.buyerFullName || item?.buyer_full_name || "").trim();
    const targetUsername = String(item?.targetUsername || item?.target_username || "")
      .trim()
      .replace(/^@+/, "");
    const amountRaw = String(item?.amountRaw ?? item?.amount ?? "").trim();
    const channelRaw = String(item?.channel || "").trim().toLowerCase();
    const channel = channelRaw || (operationSource === "miniapp" ? "miniapp_test" : operationSource);
    const status = normalizeHistoryStatus(item);
    return {
      type,
      itemType: type,
      main: normalizeHistoryMain(item, type),
      amountRaw,
      price: priceText,
      priceRub,
      priceUsd,
      status,
      promoCode,
      promoDiscount,
      promoError,
      operationId,
      operationSource,
      operationNumericId,
      buyerUserId,
      buyerUsername,
      buyerFullName,
      targetUsername,
      channel,
      createdAt: item?.createdAt || new Date().toISOString(),
    };
  };

  const mergeHistoryCollections = (primaryItems, secondaryItems) => {
    const merged = [];
    const seen = new Set();
    const appendUnique = (list) => {
      (Array.isArray(list) ? list : []).forEach((item) => {
        const normalized = normalizeHistoryEntry(item);
        const identity = normalized.operationId
          ? `op:${normalized.operationId}`
          : `${normalized.type}|${normalized.main}|${normalized.price}|${normalized.status}|${normalized.createdAt}`;
        if (seen.has(identity)) {
          return;
        }
        seen.add(identity);
        merged.push(normalized);
      });
    };

    appendUnique(primaryItems);
    appendUnique(secondaryItems);
    merged.sort((left, right) => {
      const leftTime = new Date(left.createdAt).getTime() || 0;
      const rightTime = new Date(right.createdAt).getTime() || 0;
      return rightTime - leftTime;
    });
    return merged.slice(0, 20);
  };

  historyItems = mergeHistoryCollections(historyItems, []);
  saveHistory(historyItems);

  const getFilteredHistoryItems = () => {
    const query = historySearchQuery.trim().toLowerCase();
    const byFilterItems = historyItems.filter((item) => {
      if (historyFilter !== "all" && item.type !== historyFilter) {
        return false;
      }
      return true;
    });

    if (!query) {
      return byFilterItems;
    }

    const byQueryItems = byFilterItems.filter((item) => {
      if (!query) {
        return true;
      }
      const searchable = [
        String(item?.main || ""),
        String(item?.price || ""),
        String(item?.createdAt || ""),
        formatHistoryDate(item?.createdAt || ""),
        Number.isFinite(Number(item?.priceRub)) ? String(item.priceRub) : "",
        String(item?.promoCode || ""),
        String(item?.operationId || ""),
        String(item?.buyerUserId || ""),
        String(item?.buyerUsername || ""),
        String(item?.buyerFullName || ""),
        String(item?.targetUsername || ""),
        String(item?.status || ""),
        String(item?.channel || ""),
      ]
        .join(" ")
        .toLowerCase();
      return searchable.includes(query);
    });

    // При активном "Все" не прячем историю полностью из-за случайного текста в поиске.
    if (historyFilter === "all" && byQueryItems.length === 0) {
      return byFilterItems;
    }

    return byQueryItems;
  };

  const parsePositiveInt = (value) => {
    const numeric = Number.parseInt(String(value || "").replace(/[^\d]/g, ""), 10);
    return Number.isFinite(numeric) ? Math.max(0, numeric) : 0;
  };

  const getGlobalAboutTotals = () => {
    const config = window.starslixMiniappConfig;
    if (!config || typeof config !== "object") {
      return null;
    }

    const source =
      config.totals && typeof config.totals === "object"
        ? config.totals
        : config;
    const totalStars = Number(source.totalStars);
    const totalPremiumMonths = Number(source.totalPremiumMonths);
    const hasStars = Number.isFinite(totalStars);
    const hasPremium = Number.isFinite(totalPremiumMonths);
    if (!hasStars && !hasPremium) {
      return null;
    }

    return {
      totalStars: hasStars ? Math.max(0, Math.round(totalStars)) : null,
      totalPremiumMonths: hasPremium ? Math.max(0, Math.round(totalPremiumMonths)) : null,
    };
  };

  const updateHomeAboutMetrics = () => {
    if (!homeAboutStarsValue && !homeAboutPremiumValue) {
      return;
    }

    const globalTotals = getGlobalAboutTotals();
    if (globalTotals) {
      if (homeAboutStarsValue && Number.isFinite(globalTotals.totalStars)) {
        homeAboutStarsValue.textContent = formatIntValue(globalTotals.totalStars);
      }
      if (homeAboutPremiumValue && Number.isFinite(globalTotals.totalPremiumMonths)) {
        homeAboutPremiumValue.textContent = formatIntValue(globalTotals.totalPremiumMonths);
      }
      return;
    }

    const starsTotal = historyItems.reduce((sum, item) => {
      if (normalizeHistoryType(item) !== "stars") {
        return sum;
      }
      return sum + parsePositiveInt(normalizeHistoryMain(item, "stars"));
    }, 0);

    const premiumTotal = historyItems.reduce((sum, item) => {
      if (normalizeHistoryType(item) !== "premium") {
        return sum;
      }
      return sum + parsePositiveInt(normalizeHistoryMain(item, "premium"));
    }, 0);

    if (homeAboutStarsValue) {
      homeAboutStarsValue.textContent = formatIntValue(starsTotal);
    }
    if (homeAboutPremiumValue) {
      homeAboutPremiumValue.textContent = formatIntValue(premiumTotal);
    }
  };

  const updateProfileOverview = () => {
    updateHomeAboutMetrics();

    const localPurchases = historyItems.reduce((count, item) => {
      return normalizeHistoryStatus(item) === "success" ? count + 1 : count;
    }, 0);
    const localSpentRub = historyItems.reduce((sum, item) => {
      if (normalizeHistoryStatus(item) !== "success") {
        return sum;
      }
      const numeric = Number(item?.priceRub);
      return Number.isFinite(numeric) ? sum + numeric : sum;
    }, 0);
    const localPromoUses = historyItems.reduce((count, item) => {
      const promoCodeValue = String(item?.promoCode || "").trim().toUpperCase();
      if (!promoCodeValue) {
        return count;
      }
      return normalizeHistoryStatus(item) === "success" ? count + 1 : count;
    }, 0);
    const localPromoSavingsRub = historyItems.reduce((sum, item) => {
      if (normalizeHistoryStatus(item) !== "success") {
        return sum;
      }
      const promoDiscount = Number(item?.promoDiscount);
      const priceRub = Number(item?.priceRub);
      if (!Number.isFinite(priceRub) || priceRub <= 0 || !Number.isFinite(promoDiscount) || promoDiscount <= 0) {
        return sum;
      }
      if (promoDiscount >= 100) {
        return sum + priceRub;
      }
      return sum + (priceRub * promoDiscount) / (100 - promoDiscount);
    }, 0);

    const totalPurchases = Number.isFinite(Number(profileStatsFromApi?.totalPurchases))
      ? Number(profileStatsFromApi.totalPurchases)
      : localPurchases;
    const totalSpentRub = Number.isFinite(Number(profileStatsFromApi?.totalSpentRub))
      ? Number(profileStatsFromApi.totalSpentRub)
      : localSpentRub;
    const totalPromos = Number.isFinite(Number(profileStatsFromApi?.usedPromos))
      ? Number(profileStatsFromApi.usedPromos)
      : localPromoUses;
    const totalPromoSavingsRub = Number.isFinite(Number(profileStatsFromApi?.promoSavingsRub))
      ? Number(profileStatsFromApi.promoSavingsRub)
      : localPromoSavingsRub;
    const usdRubRate = Number(window.starslixMiniappConfig?.usdRubRate);
    const safeUsdRubRate = Number.isFinite(usdRubRate) && usdRubRate > 0 ? usdRubRate : 76.5;
    const totalPromoSavingsUsd = Number.isFinite(Number(profileStatsFromApi?.promoSavingsUsd))
      ? Number(profileStatsFromApi.promoSavingsUsd)
      : totalPromoSavingsRub / safeUsdRubRate;

    if (profileStatPurchasesValue) {
      profileStatPurchasesValue.textContent = formatIntValue(totalPurchases);
    }
    if (profileStatSpentValue) {
      profileStatSpentValue.textContent = formatRubValue(totalSpentRub);
    }
    if (profileStatPromosValue) {
      profileStatPromosValue.textContent = formatIntValue(totalPromos);
    }
    if (profileStatPromosSavings) {
      profileStatPromosSavings.textContent = isEnglishLocale()
        ? `Saved: ${formatRubValue(totalPromoSavingsRub)} / ${formatUsdValue(totalPromoSavingsUsd)}`
        : `Экономия: ${formatRubValue(totalPromoSavingsRub)} / ${formatUsdValue(totalPromoSavingsUsd)}`;
    }

    const spentBasedLevel = getProfileLevelInfoBySpent(totalSpentRub);
    const apiLevelInfo = profileStatsFromApi?.levelInfo;
    const apiFixedRate = Number(apiLevelInfo?.fixedStarRateRub);
    const apiNextTargetRaw =
      apiLevelInfo?.nextLevelTargetRub !== undefined
        ? apiLevelInfo?.nextLevelTargetRub
        : apiLevelInfo?.nextTarget;
    const apiNextTarget = Number(apiNextTargetRaw);
    const levelInfo =
      apiLevelInfo && typeof apiLevelInfo === "object"
        ? {
            level: Math.max(1, Math.min(4, Number(apiLevelInfo.level) || spentBasedLevel.level)),
            minSpent: Number.isFinite(Number(apiLevelInfo.minSpentRub))
              ? Number(apiLevelInfo.minSpentRub)
              : Number.isFinite(Number(apiLevelInfo.minSpent))
              ? Number(apiLevelInfo.minSpent)
              : spentBasedLevel.minSpent,
            nextTarget:
              apiNextTargetRaw === null
                ? null
                : Number.isFinite(apiNextTarget)
                ? apiNextTarget
                : spentBasedLevel.nextTarget,
            cashbackPercent: Math.max(
              0,
              Number.isFinite(Number(apiLevelInfo.cashbackPercent))
                ? Number(apiLevelInfo.cashbackPercent)
                : spentBasedLevel.cashbackPercent
            ),
            personalOffers:
              typeof apiLevelInfo.personalOffers === "boolean"
                ? apiLevelInfo.personalOffers
                : spentBasedLevel.personalOffers,
            fixedStarRateRub:
              Number.isFinite(apiFixedRate) && apiFixedRate > 0
                ? apiFixedRate
                : spentBasedLevel.fixedStarRateRub,
          }
        : spentBasedLevel;
    syncProfilePerksToWindow(levelInfo);

    const nextLevelTarget = levelInfo.nextTarget;
    const previousLevelTarget = Math.max(0, Number(levelInfo.minSpent) || 0);
    const levelLabel = isEnglishLocale()
      ? `Level ${levelInfo.level}`
      : `Уровень ${levelInfo.level}`;
    if (profileProgressLevel) {
      profileProgressLevel.textContent = levelLabel;
    }

    const safeSpentRub = Math.max(0, Number(totalSpentRub) || 0);
    const perksText = buildProfilePerksText(levelInfo);
    let progressPercent = 100;
    let progressNote = isEnglishLocale()
      ? `Maximum level reached • ${perksText}`
      : `Максимальный уровень достигнут • ${perksText}`;
    if (nextLevelTarget !== null) {
      const segment = Math.max(1, nextLevelTarget - previousLevelTarget);
      const filled = Math.max(0, Math.min(segment, safeSpentRub - previousLevelTarget));
      progressPercent = Math.round((filled / segment) * 100);
      const remaining = Math.max(0, nextLevelTarget - safeSpentRub);
      progressNote = isEnglishLocale()
        ? `To level ${Math.min(4, levelInfo.level + 1)}: ${formatRubValue(remaining)} • ${perksText}`
        : `До уровня ${Math.min(4, levelInfo.level + 1)}: ${formatRubValue(remaining)} • ${perksText}`;
    }
    if (profileProgressFill) {
      profileProgressFill.style.width = `${Math.max(0, Math.min(100, progressPercent))}%`;
    }
    if (profileProgressNote) {
      profileProgressNote.textContent = progressNote;
    }
  };

  const updateHistoryPaginationUi = (totalPages) => {
    if (!profileHistoryControls || !profileHistoryPrev || !profileHistoryNext || !profileHistoryPage) {
      return;
    }

    const safePages = Math.max(1, totalPages);
    profileHistoryControls.hidden = safePages <= 1;
    profileHistoryPrev.disabled = historyPage <= 0;
    profileHistoryNext.disabled = historyPage >= safePages - 1;
    profileHistoryPage.textContent = `${historyPage + 1} / ${safePages}`;
  };

  const getHistoryTypeTitle = (type) => {
    if (isEnglishLocale()) {
      if (type === "premium") {
        return "Premium";
      }
      if (type === "ton") {
        return "TON";
      }
      return "Stars";
    }
    if (type === "premium") {
      return "Premium";
    }
    if (type === "ton") {
      return "TON";
    }
    return "Звёзды";
  };

  const getHistoryChannelTitle = (item) => {
    const channelRaw = String(item?.channel || item?.operationSource || "").trim().toLowerCase();
    if (channelRaw === "legacy") {
      return isEnglishLocale() ? "Legacy bot" : "Старый бот";
    }
    if (channelRaw === "miniapp_test" || channelRaw === "miniapp") {
      return isEnglishLocale() ? "Miniapp test" : "Miniapp тест";
    }
    return channelRaw || "—";
  };

  const getHistoryBuyerTitle = (item) => {
    const parts = [];
    const usernameRaw = String(item?.buyerUsername || "").trim().replace(/^@+/, "");
    if (usernameRaw) {
      parts.push(`@${usernameRaw}`);
    }
    const fullName = String(item?.buyerFullName || "").trim();
    if (fullName) {
      parts.push(fullName);
    }
    const userIdText = String(item?.buyerUserId || "").trim();
    if (userIdText) {
      parts.push(`ID ${userIdText}`);
    }
    return parts.length ? parts.join(" • ") : "—";
  };

  const copyTextToClipboard = async (rawValue) => {
    const text = String(rawValue || "").trim();
    if (!text || text === "—") {
      return false;
    }
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
        return true;
      }
    } catch {}

    try {
      const helper = document.createElement("textarea");
      helper.value = text;
      helper.setAttribute("readonly", "readonly");
      helper.style.position = "fixed";
      helper.style.opacity = "0";
      helper.style.pointerEvents = "none";
      document.body.appendChild(helper);
      helper.select();
      const copied = document.execCommand("copy");
      helper.remove();
      return Boolean(copied);
    } catch {
      return false;
    }
  };

  const historyModalRoot = document.createElement("div");
  historyModalRoot.className = "profile-history-modal";
  historyModalRoot.hidden = true;
  historyModalRoot.innerHTML = `
    <div class="profile-history-modal-backdrop" data-history-modal-close></div>
    <section class="profile-history-modal-card" role="dialog" aria-modal="true" aria-label="Детали операции">
      <div class="profile-history-modal-head">
        <button class="profile-history-modal-back" type="button" data-history-modal-close>Назад</button>
        <div class="profile-history-modal-title">Детали операции</div>
      </div>
      <div class="profile-history-modal-body">
        <button class="profile-history-modal-operation-id" id="profileHistoryModalOperationId" type="button">ID: —</button>
        <div class="profile-history-modal-grid" id="profileHistoryModalGrid"></div>
      </div>
    </section>
  `;
  document.body.appendChild(historyModalRoot);

  const historyModalOperationId = document.getElementById("profileHistoryModalOperationId");
  const historyModalGrid = document.getElementById("profileHistoryModalGrid");
  let historyModalCopyTimer = null;

  const closeHistoryOperationModal = () => {
    historyModalRoot.classList.remove("is-open");
    historyModalRoot.hidden = true;
    document.body.classList.remove("is-history-modal-open");
    if (historyModalCopyTimer) {
      window.clearTimeout(historyModalCopyTimer);
      historyModalCopyTimer = null;
    }
    historyModalOperationId?.classList.remove("is-copied");
  };

  const openHistoryOperationModal = (payload) => {
    if (!historyModalOperationId || !historyModalGrid) {
      return;
    }
    const operationId = String(payload?.operationId || "").trim().toUpperCase() || "—";
    const statusText = String(payload?.statusText || "—");
    const typeText = String(payload?.typeText || "—");
    const amountText = String(payload?.amountText || "—");
    const priceText = String(payload?.priceText || "—");
    const promoText = String(payload?.promoText || "—");
    const dateText = String(payload?.dateText || "—");
    const buyerText = String(payload?.buyerText || "—");
    const recipientText = String(payload?.recipientText || "—");
    const channelText = String(payload?.channelText || "—");

    historyModalOperationId.dataset.copyValue = operationId;
    historyModalOperationId.classList.remove("is-copied");
    historyModalOperationId.textContent = `ID: ${operationId}`;

    const rows = [
      [isEnglishLocale() ? "Product" : "Товар", typeText],
      [isEnglishLocale() ? "Quantity" : "Количество", amountText],
      [isEnglishLocale() ? "Status" : "Статус", statusText],
      [isEnglishLocale() ? "Price" : "Стоимость", priceText],
      [isEnglishLocale() ? "Promo" : "Промокод", promoText],
      [isEnglishLocale() ? "Date" : "Дата и время", dateText],
      [isEnglishLocale() ? "Buyer" : "Покупатель", buyerText],
      [isEnglishLocale() ? "Recipient" : "Получатель", recipientText],
      [isEnglishLocale() ? "Channel" : "Канал", channelText],
    ];

    historyModalGrid.innerHTML = rows
      .map(
        ([label, value]) =>
          `<div class="profile-history-modal-row"><span class="profile-history-modal-label">${escapeHtml(label)}</span><span class="profile-history-modal-value">${escapeHtml(value)}</span></div>`
      )
      .join("");

    historyModalRoot.hidden = false;
    document.body.classList.add("is-history-modal-open");
    window.requestAnimationFrame(() => {
      historyModalRoot.classList.add("is-open");
    });
  };

  historyModalRoot.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }
    if (target.closest("[data-history-modal-close]")) {
      closeHistoryOperationModal();
    }
  });

  historyModalOperationId?.addEventListener("click", async () => {
    const value = String(historyModalOperationId.dataset.copyValue || "").trim();
    const copied = await copyTextToClipboard(value);
    if (!copied) {
      return;
    }
    historyModalOperationId.classList.add("is-copied");
    historyModalOperationId.textContent = isEnglishLocale()
      ? `ID: ${value} (Copied)`
      : `ID: ${value} (Скопировано)`;
    if (historyModalCopyTimer) {
      window.clearTimeout(historyModalCopyTimer);
    }
    historyModalCopyTimer = window.setTimeout(() => {
      historyModalOperationId.classList.remove("is-copied");
      historyModalOperationId.textContent = `ID: ${value}`;
      historyModalCopyTimer = null;
    }, 1300);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !historyModalRoot.hidden) {
      closeHistoryOperationModal();
    }
  });

  const renderHistory = () => {
    if (!profileHistoryList || !profileHistoryEmpty) {
      return;
    }

    updateProfileOverview();
    profileHistoryList.querySelectorAll(".profile-history-item").forEach((node) => node.remove());
    const filteredHistoryItems = getFilteredHistoryItems();
    const totalPages = Math.max(1, Math.ceil(filteredHistoryItems.length / historyPageSize));
    historyPage = Math.min(Math.max(0, historyPage), totalPages - 1);

    if (!filteredHistoryItems.length) {
      const apiPurchasesCount = Number(profileStatsFromApi?.totalPurchases);
      const localHasAnyItems = historyItems.length > 0;
      const apiHasAnyItems = Number.isFinite(apiPurchasesCount) && apiPurchasesCount > 0;
      const hasSearch = Boolean(historySearchQuery.trim());
      const isAllView = historyFilter === "all" && !hasSearch;

      if (isAllView && (localHasAnyItems || apiHasAnyItems)) {
        profileHistoryEmpty.hidden = true;
        if (profileHistoryEmptyAction) {
          profileHistoryEmptyAction.hidden = true;
        }
        updateHistoryPaginationUi(1);
        return;
      }
      const emptyTextNode = profileHistoryEmpty.querySelector(".profile-history-empty");
      if (emptyTextNode) {
        emptyTextNode.textContent = isEnglishLocale() ? "No purchases yet" : "Покупок пока нет";
      }
      if (profileHistoryEmptyAction) {
        profileHistoryEmptyAction.hidden = localHasAnyItems || apiHasAnyItems;
      }
      profileHistoryEmpty.hidden = false;
      updateHistoryPaginationUi(1);
      return;
    }

    profileHistoryEmpty.hidden = true;
    if (profileHistoryEmptyAction) {
      profileHistoryEmptyAction.hidden = true;
    }

    const pageStart = historyPage * historyPageSize;
    const visibleItems = filteredHistoryItems.slice(pageStart, pageStart + historyPageSize);

    visibleItems.forEach((item, index) => {
      const type = normalizeHistoryType(item);
      const iconSrc = historyTypeMeta[type]?.icon || historyTypeMeta.stars.icon;
      const status = normalizeHistoryStatus(item);
      const typeTitle = getHistoryTypeTitle(type);
      const dateText = formatHistoryDate(item.createdAt) || "—";
      const operationId = String(item?.operationId || "").trim().toUpperCase() || "—";
      const targetUsernameRaw = String(item?.targetUsername || "").trim().replace(/^@+/, "");
      const targetUsername = targetUsernameRaw ? `@${targetUsernameRaw}` : "—";
      const promoCode = String(item?.promoCode || "").trim().toUpperCase();
      const promoDiscountValue = Number(item?.promoDiscount);
      const promoSuffix =
        promoCode && Number.isFinite(promoDiscountValue) && promoDiscountValue > 0
          ? ` (${promoDiscountValue}%)`
          : "";
      const promoText = promoCode ? `${promoCode}${promoSuffix}` : "—";
      const channelText = getHistoryChannelTitle(item);
      const buyerText = getHistoryBuyerTitle(item);
      const amountRaw = String(item?.amountRaw || "").trim();
      const amountText = amountRaw || normalizeHistoryMain(item, type);

      const row = document.createElement("div");
      row.className = `profile-history-item profile-history-item--${type} profile-history-item--status-${status}`;
      row.style.setProperty("--history-item-delay", `${Math.min(index, 6) * 18}ms`);
      row.setAttribute("role", "button");
      row.setAttribute("tabindex", "0");

      const iconWrap = document.createElement("span");
      iconWrap.className = "profile-history-icon-wrap";
      iconWrap.setAttribute("aria-hidden", "true");

      const icon = document.createElement("img");
      icon.className = "profile-history-icon";
      icon.src = iconSrc;
      icon.alt = "";
      iconWrap.appendChild(icon);

      const main = document.createElement("span");
      main.className = "profile-history-main";
      main.textContent = normalizeHistoryMain(item, type);

      const statusNode = document.createElement("span");
      statusNode.className = `profile-history-status profile-history-status--${status}`;
      statusNode.textContent = getHistoryStatusLabel(status);

      const side = document.createElement("span");
      side.className = "profile-history-side";

      const price = document.createElement("span");
      price.className = "profile-history-price";
      price.textContent = item.price || "—";

      const date = document.createElement("span");
      date.className = "profile-history-date";
      date.textContent = dateText || "—";

      row.addEventListener("click", (event) => {
        const target = event.target;
        if (
          target instanceof HTMLElement &&
          target.closest("a, button, input, textarea, select, label")
        ) {
          return;
        }
        openHistoryOperationModal({
          operationId,
          typeText: typeTitle,
          amountText: amountText || "—",
          priceText: String(item.price || "—"),
          promoText,
          statusText: getHistoryStatusLabel(status),
          dateText,
          buyerText,
          recipientText: targetUsername,
          channelText,
        });
      });

      row.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") {
          return;
        }
        event.preventDefault();
        openHistoryOperationModal({
          operationId,
          typeText: typeTitle,
          amountText: amountText || "—",
          priceText: String(item.price || "—"),
          promoText,
          statusText: getHistoryStatusLabel(status),
          dateText,
          buyerText,
          recipientText: targetUsername,
          channelText,
        });
      });

      side.append(price, date);
      row.append(iconWrap, main, side, statusNode);
      profileHistoryList.appendChild(row);
    });

    updateHistoryPaginationUi(totalPages);
  };

  const addCurrentOrderToHistory = (override = null) => {
    const activeView = document.querySelector(".panel-view.is-active");
    if (!activeView) {
      return;
    }

    const activeCard =
      activeView.querySelector(".offer-card.is-active") || activeView.querySelector(".offer-card");
    if (!activeCard) {
      return;
    }

    const viewName = activeView.dataset.view || "";
    const purchaseType =
      viewName === "premium" || viewName === "ton" ? viewName : "stars";
    const priceUsdText = activeView.querySelector("[data-price-usd]")?.textContent?.trim() || "";
    const priceRubText = activeView.querySelector("[data-price-rub]")?.textContent?.trim() || "";

    const formatRubHistory = (value) => {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {
        return "";
      }
      return `${Number.isInteger(numeric) ? numeric : numeric.toFixed(2)}₽`;
    };

    const formatUsdHistory = (value) => {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {
        return "";
      }
      return `${numeric.toFixed(2)}$`;
    };

    const cardAmountText = activeCard.querySelector(".offer-card-amount")?.textContent?.trim() || "";
    let amountText = cardAmountText;

    if (purchaseType === "stars") {
      const starsInput = activeView.querySelector('.form-input[type="number"]');
      const starsInputValue = String(starsInput?.value || "").trim();
      if (starsInputValue) {
        amountText = starsInputValue;
      }
    }

    if (override?.amount) {
      if (purchaseType === "premium") {
        amountText = `${override.amount} мес.`;
      } else {
        amountText = String(override.amount);
      }
    }

    let priceText = activeCard.querySelector(".offer-card-price")?.textContent?.trim() || "—";
    if (priceUsdText && priceRubText) {
      priceText = `${priceUsdText}/${priceRubText}`;
    }
    if (
      override &&
      override.priceUsd !== undefined &&
      override.priceRub !== undefined
    ) {
      const usd = formatUsdHistory(override.priceUsd);
      const rub = formatRubHistory(override.priceRub);
      priceText = usd && rub ? `${usd}/${rub}` : priceText;
    }

    const entryPriceRub =
      override && override.priceRub !== undefined
        ? Number(override.priceRub)
        : extractRubFromPriceLabel(priceText);
    const entryPromoCode = String(override?.promoCode || "").trim().toUpperCase();
    const entryPromoError = String(override?.promoError || "").trim();
    const entryPromoDiscount = Number(override?.promoDiscount);
    const entryCreatedAt = String(override?.createdAt || "").trim() || new Date().toISOString();
    const entryStatus = normalizeHistoryStatus({
      status: override?.status,
      promoError: entryPromoError,
    });
    const entryOperationId = String(override?.operationId || "").trim().toUpperCase();
    const entryOperationSource = String(override?.operationSource || "").trim().toLowerCase();
    const entryOperationNumericId = Number.parseInt(String(override?.operationNumericId || ""), 10);
    const entryBuyerUserId = Number.parseInt(String(override?.buyerUserId || ""), 10);
    const entryBuyerUsername = String(override?.buyerUsername || "").trim().replace(/^@+/, "");
    const entryBuyerFullName = String(override?.buyerFullName || "").trim();
    const currentTargetUsername = String(
      activeView.querySelector(".form-input--telegram")?.value || ""
    )
      .trim()
      .replace(/^@+/, "");
    const entryTargetUsername = String(override?.targetUsername || currentTargetUsername || "")
      .trim()
      .replace(/^@+/, "");
    const entryChannel = String(override?.channel || "").trim().toLowerCase();

    if (profileStatsFromApi && typeof profileStatsFromApi === "object") {
      const currentPurchases = Number(profileStatsFromApi.totalPurchases);
      const currentSpent = Number(profileStatsFromApi.totalSpentRub);
      const currentUsedPromos = Number(profileStatsFromApi.usedPromos);
      const currentPromoSavingsRub = Number(profileStatsFromApi.promoSavingsRub);
      const currentPromoSavingsUsd = Number(profileStatsFromApi.promoSavingsUsd);
      const promoDiscountValue = entryPromoDiscount;
      const purchaseIncrement = entryStatus === "success" ? 1 : 0;
      const spentIncrement =
        entryStatus === "success" && Number.isFinite(entryPriceRub) ? entryPriceRub : 0;
      const promoIncrement = entryPromoCode && entryStatus === "success" ? 1 : 0;
      const promoSavingsIncrementRub =
        entryStatus === "success" &&
        Number.isFinite(entryPriceRub) &&
        Number.isFinite(promoDiscountValue) &&
        promoDiscountValue > 0 &&
        promoDiscountValue < 100
          ? (entryPriceRub * promoDiscountValue) / (100 - promoDiscountValue)
          : 0;
      const usdRubRate = Number(window.starslixMiniappConfig?.usdRubRate);
      const safeUsdRubRate = Number.isFinite(usdRubRate) && usdRubRate > 0 ? usdRubRate : 76.5;
      const promoSavingsIncrementUsd = promoSavingsIncrementRub / safeUsdRubRate;
      const nextTotalSpent = Number.isFinite(currentSpent)
        ? Number((currentSpent + spentIncrement).toFixed(2))
        : currentSpent;
      profileStatsFromApi = {
        ...profileStatsFromApi,
        totalPurchases:
          Number.isFinite(currentPurchases) ? currentPurchases + purchaseIncrement : currentPurchases,
        totalSpentRub: nextTotalSpent,
        usedPromos: Number.isFinite(currentUsedPromos) ? currentUsedPromos + promoIncrement : currentUsedPromos,
        promoSavingsRub: Number.isFinite(currentPromoSavingsRub)
          ? Number((currentPromoSavingsRub + promoSavingsIncrementRub).toFixed(2))
          : currentPromoSavingsRub,
        promoSavingsUsd: Number.isFinite(currentPromoSavingsUsd)
          ? Number((currentPromoSavingsUsd + promoSavingsIncrementUsd).toFixed(2))
          : currentPromoSavingsUsd,
        levelInfo: Number.isFinite(nextTotalSpent)
          ? getProfileLevelInfoBySpent(nextTotalSpent)
          : profileStatsFromApi.levelInfo,
      };
    }

    historyItems = mergeHistoryCollections(
      [
        {
          type: purchaseType,
          main: amountText || "—",
          amountRaw: String(override?.amount ?? amountText ?? "").trim(),
          price: priceText,
          priceRub: entryPriceRub,
          priceUsd:
            override && override.priceUsd !== undefined && Number.isFinite(Number(override.priceUsd))
              ? Number(override.priceUsd)
              : null,
          status: entryStatus,
          promoCode: entryPromoCode,
          promoDiscount: Number.isFinite(entryPromoDiscount) ? entryPromoDiscount : 0,
          promoError: entryPromoError,
          operationId: entryOperationId,
          operationSource:
            entryOperationSource === "miniapp" || entryOperationSource === "legacy"
              ? entryOperationSource
              : "",
          operationNumericId:
            Number.isFinite(entryOperationNumericId) && entryOperationNumericId > 0
              ? entryOperationNumericId
              : 0,
          buyerUserId:
            Number.isFinite(entryBuyerUserId) && entryBuyerUserId > 0 ? entryBuyerUserId : 0,
          buyerUsername: entryBuyerUsername,
          buyerFullName: entryBuyerFullName,
          targetUsername: entryTargetUsername,
          channel: entryChannel,
          createdAt: entryCreatedAt,
        },
      ],
      historyItems
    );

    saveHistory(historyItems);
    historyPage = 0;
    renderHistory();
  };

  const apiMeta = document.querySelector('meta[name="starslix-api-base"]');
  const apiBaseRaw = (apiMeta?.content || "").trim().replace(/\/+$/, "");
  const isMixedContentBlocked =
    window.location.protocol === "https:" && /^http:\/\//i.test(apiBaseRaw);
  const apiBase = isMixedContentBlocked ? "" : apiBaseRaw;
  const buildApiUrl = (path) => (apiBase ? `${apiBase}${path}` : path);
  const profileStorageKey = "starslix:profile-user";
  let currentUserIsAdmin = false;

  const showMiniappMessage = (text) => {
    if (window.Telegram?.WebApp?.showAlert) {
      window.Telegram.WebApp.showAlert(text);
      return;
    }
    window.alert(text);
  };

  const setAdminPanelVisibility = (isAdmin) => {
    currentUserIsAdmin = Boolean(isAdmin);
    if (profileAdminActions) {
      profileAdminActions.hidden = !currentUserIsAdmin;
    }
    if (profileAdminStatus) {
      profileAdminStatus.textContent = isEnglishLocale()
        ? "Access to miniapp management"
        : "Доступ к управлению miniapp";
    }
    if (profileAdminButton) {
      profileAdminButton.disabled = !currentUserIsAdmin;
      profileAdminButton.setAttribute("aria-hidden", currentUserIsAdmin ? "false" : "true");
    }
  };

  const parseUserFromInitData = (initDataRaw) => {
    if (!initDataRaw) {
      return null;
    }
    try {
      const params = new URLSearchParams(initDataRaw);
      const rawUser = params.get("user");
      if (!rawUser) {
        return null;
      }
      return JSON.parse(rawUser);
    } catch {
      return null;
    }
  };

  const normalizeProfileUser = (rawUser) => {
    if (!rawUser || typeof rawUser !== "object") {
      return null;
    }
    const firstName = String(rawUser.first_name ?? rawUser.firstName ?? "").trim();
    const lastName = String(rawUser.last_name ?? rawUser.lastName ?? "").trim();
    const usernameRaw = String(rawUser.username ?? "").trim().replace(/^@+/, "");
    const username = usernameRaw ? `@${usernameRaw}` : "";
    const telegramId = rawUser.id ? String(rawUser.id) : "";
    const fullName = String(rawUser.fullName ?? "").trim() || [firstName, lastName].filter(Boolean).join(" ").trim();
    const photoUrl = String(rawUser.photo_url ?? rawUser.photoUrl ?? "").trim();
    return {
      firstName,
      lastName,
      fullName,
      username,
      telegramId,
      photoUrl,
    };
  };

  const loadStoredProfileUser = () => {
    try {
      const raw = window.localStorage.getItem(profileStorageKey);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  };

  const saveStoredProfileUser = (profileUser) => {
    if (!profileUser) {
      return;
    }
    try {
      window.localStorage.setItem(profileStorageKey, JSON.stringify(profileUser));
    } catch {
      // ignore storage write errors
    }
  };

  const mergeProfileUsers = (baseUser, nextUser) => {
    const base = baseUser || {};
    const next = nextUser || {};
    return {
      firstName: next.firstName || base.firstName || "",
      lastName: next.lastName || base.lastName || "",
      fullName: next.fullName || base.fullName || "",
      username: next.username || base.username || "",
      telegramId: next.telegramId || base.telegramId || "",
      photoUrl: next.photoUrl || base.photoUrl || "",
    };
  };

  const applyProfileFields = (profileUser) => {
    const user = profileUser || {};
    if (profileUsername) {
      profileUsername.textContent = user.username || "—";
    }
    if (profileTelegramId) {
      profileTelegramId.textContent = user.telegramId || "—";
    }
    if (profileFullname) {
      const fullName = user.fullName || [user.firstName, user.lastName].filter(Boolean).join(" ").trim();
      if (fullName) {
        profileFullname.textContent = fullName;
        profileFullname.dataset.empty = "false";
      }
    }
  };

  const webAppUser = normalizeProfileUser(
    window.Telegram?.WebApp?.initDataUnsafe?.user ||
      parseUserFromInitData(window.Telegram?.WebApp?.initData || "")
  );
  const storedProfileUser = normalizeProfileUser(loadStoredProfileUser());
  let currentProfileUser = mergeProfileUsers(storedProfileUser, webAppUser);

  applyProfileFields(currentProfileUser);
  setAdminPanelVisibility(false);
  if (webAppUser) {
    saveStoredProfileUser(currentProfileUser);
  }

  const applyAvatarSrc = (src) => {
    appAvatarNodes.forEach((avatarNode) => {
      if (!(avatarNode instanceof HTMLImageElement)) {
        return;
      }
      const fallbackSrc =
        avatarNode.dataset.fallbackSrc ||
        avatarNode.getAttribute("src") ||
        "avauser.png";
      avatarNode.dataset.fallbackSrc = fallbackSrc;
      avatarNode.onerror = () => {
        if (avatarNode.getAttribute("src") !== fallbackSrc) {
          avatarNode.src = fallbackSrc;
        }
      };
      avatarNode.src = src || fallbackSrc;
    });
  };

  const avatarStorageKey = "starslix:avatar-url";
  let storedAvatarUrl = "";
  try {
    storedAvatarUrl = window.localStorage.getItem(avatarStorageKey) || "";
  } catch (error) {}

  const currentAvatarUrl =
    typeof currentProfileUser?.photoUrl === "string" ? currentProfileUser.photoUrl.trim() : "";

  if (currentAvatarUrl) {
    try {
      window.localStorage.setItem(avatarStorageKey, currentAvatarUrl);
    } catch (error) {}
  }

  applyAvatarSrc(currentAvatarUrl || storedAvatarUrl || "");

  const applyHistoryFromApi = (rawHistory) => {
    if (!Array.isArray(rawHistory)) {
      return;
    }
    const normalizedApiItems = rawHistory.map((item) => normalizeHistoryEntry(item)).slice(0, 20);
    if (normalizedApiItems.length > 0) {
      historyItems = normalizedApiItems;
    } else {
      historyItems = mergeHistoryCollections(historyItems, []);
    }
    saveHistory(historyItems);
    if (historyFilter === "all" && historySearchQuery) {
      historySearchQuery = "";
      if (profileHistorySearchInput) {
        profileHistorySearchInput.value = "";
      }
    }
    historyPage = 0;
    renderHistory();
  };

  const fetchProfileFromApi = async () => {
    const initData = window.Telegram?.WebApp?.initData || "";
    if (!initData) {
      return;
    }

    try {
      const response = await fetch(buildApiUrl("/api/miniapp/profile"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({ initData }),
      });

      if (!response.ok) {
        return;
      }

      const data = await response.json().catch(() => null);
      if (!data?.ok) {
        return;
      }

      setAdminPanelVisibility(Boolean(data.isAdmin));
      if (data.stats && typeof data.stats === "object") {
        const levelInfoRaw = data.stats.levelInfo && typeof data.stats.levelInfo === "object"
          ? data.stats.levelInfo
          : null;
        profileStatsFromApi = {
          totalPurchases: Number(data.stats.totalPurchases),
          totalSpentRub: Number(data.stats.totalSpentRub),
          usedPromos: Number(data.stats.usedPromos),
          promoSavingsRub: Number(data.stats.promoSavingsRub),
          promoSavingsUsd: Number(data.stats.promoSavingsUsd),
          levelInfo: levelInfoRaw
            ? {
                level: Number(levelInfoRaw.level),
                minSpentRub: Number(levelInfoRaw.minSpentRub),
                nextLevelTargetRub:
                  levelInfoRaw.nextLevelTargetRub === null
                    ? null
                    : Number(levelInfoRaw.nextLevelTargetRub),
                cashbackPercent: Number(levelInfoRaw.cashbackPercent),
                personalOffers: Boolean(levelInfoRaw.personalOffers),
                fixedStarRateRub:
                  levelInfoRaw.fixedStarRateRub === null
                    ? null
                    : Number(levelInfoRaw.fixedStarRateRub),
              }
            : null,
        };
      }

      const apiUser = normalizeProfileUser(data.user || {});
      if (apiUser) {
        currentProfileUser = mergeProfileUsers(currentProfileUser, apiUser);
        applyProfileFields(currentProfileUser);
        saveStoredProfileUser(currentProfileUser);

        if (currentProfileUser.photoUrl) {
          try {
            window.localStorage.setItem(avatarStorageKey, currentProfileUser.photoUrl);
          } catch {
            // ignore storage write errors
          }
          applyAvatarSrc(currentProfileUser.photoUrl);
        }
      }

      applyHistoryFromApi(data.history);
      updateProfileOverview();
    } catch {
      // silent fallback to cached local profile/history
    }
  };

  if (avatarToggle) {
    avatarToggle.removeAttribute("disabled");
    avatarToggle.removeAttribute("aria-disabled");
    avatarToggle.type = "button";
    avatarToggle.setAttribute("aria-expanded", "false");
  }
  if (quickMenu) {
    quickMenu.classList.add("is-open");
    quickMenu.setAttribute("aria-hidden", "false");
  }

  avatarToggle?.addEventListener("click", (event) => {
    event.preventDefault();
    window.dispatchEvent(
      new CustomEvent("starslix:navigate", {
        detail: { screen: "profile" },
      })
    );
  });

  profileCloseButton?.addEventListener("click", (event) => {
    event.preventDefault();
    window.dispatchEvent(
      new CustomEvent("starslix:navigate", {
        detail: { screen: "home" },
      })
    );
  });

  profileHistoryPrev?.addEventListener("click", (event) => {
    event.preventDefault();
    if (historyPage <= 0) {
      return;
    }
    historyPage -= 1;
    renderHistory();
  });

  profileHistoryNext?.addEventListener("click", (event) => {
    event.preventDefault();
    const totalPages = Math.max(1, Math.ceil(getFilteredHistoryItems().length / historyPageSize));
    if (historyPage >= totalPages - 1) {
      return;
    }
    historyPage += 1;
    renderHistory();
  });

  profileHistoryFilters.forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      const nextFilter = String(button.dataset.historyFilter || "all").trim().toLowerCase();
      historyFilter = ["all", "stars", "premium", "ton"].includes(nextFilter) ? nextFilter : "all";
      if (historyFilter === "all") {
        historySearchQuery = "";
        if (profileHistorySearchInput) {
          profileHistorySearchInput.value = "";
        }
      }
      historyPage = 0;
      profileHistoryFilters.forEach((node) => {
        node.classList.toggle("is-active", node === button);
      });
      renderHistory();
    });
  });

  const applyHistorySearch = () => {
    historySearchQuery = String(profileHistorySearchInput?.value || "").trim();
    historyPage = 0;
    renderHistory();
  };

  profileHistorySearchButton?.addEventListener("click", (event) => {
    event.preventDefault();
    applyHistorySearch();
  });

  profileHistorySearchInput?.addEventListener("input", () => {
    applyHistorySearch();
  });

  profileHistorySearchInput?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") {
      return;
    }
    event.preventDefault();
    applyHistorySearch();
  });

  profileHistoryEmptyAction?.addEventListener("click", (event) => {
    event.preventDefault();
    window.dispatchEvent(
      new CustomEvent("starslix:navigate", {
        detail: { screen: "home" },
      })
    );
  });

  profileAdminButton?.addEventListener("click", (event) => {
    event.preventDefault();
    if (!currentUserIsAdmin) {
      return;
    }
    window.dispatchEvent(
      new CustomEvent("starslix:navigate", {
        detail: { screen: "admin" },
      })
    );
  });

  window.addEventListener("starslix:language-updated", () => {
    setAdminPanelVisibility(currentUserIsAdmin);
    renderHistory();
  });

  window.addEventListener("starslix:config-updated", () => {
    updateHomeAboutMetrics();
  });

  let profileRefreshInFlight = false;
  const refreshProfileData = async (force = false) => {
    const isProfileActive = profilePage.classList.contains("is-active");
    if (!force && (!isProfileActive || document.hidden)) {
      return;
    }
    if (profileRefreshInFlight) {
      return;
    }
    profileRefreshInFlight = true;
    try {
      await fetchProfileFromApi();
    } finally {
      profileRefreshInFlight = false;
    }
  };

  window.addEventListener("starslix:screen-changed", (event) => {
    const screen = String(event?.detail?.screen || "");
    if (screen !== "profile") {
      return;
    }
    refreshProfileData(true);
  });

  window.addEventListener("starslix:server-update", (event) => {
    const payload = event?.detail || {};
    const currentUserId = String(currentProfileUser?.telegramId || "").trim();
    const payloadUserId = String(payload?.userId || payload?.user_id || "").trim();
    if (!currentUserId) {
      return;
    }
    if (payloadUserId && payloadUserId !== currentUserId) {
      return;
    }
    refreshProfileData(true);
  });

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && profilePage.classList.contains("is-active")) {
      refreshProfileData(true);
    }
  });

  // Экспортируем, чтобы добавлять запись после успешной отправки заказа на backend.
  window.starslixAddCurrentOrderToHistory = addCurrentOrderToHistory;

  setProfilePageState(false);
  renderHistory();
  refreshProfileData(true);
})();

(() => {
  const telegramBotButton = document.getElementById("telegramBotButton");
  if (!telegramBotButton) {
    return;
  }

  const telegramBotUrl = "https://t.me/starslixbot";

  telegramBotButton.addEventListener("click", (event) => {
    event.preventDefault();

    if (window.Telegram?.WebApp?.openTelegramLink) {
      window.Telegram.WebApp.openTelegramLink(telegramBotUrl);
      return;
    }

    window.open(telegramBotUrl, "_blank", "noopener,noreferrer");
  });
})();

(() => {
  const apiMeta = document.querySelector('meta[name="starslix-api-base"]');
  const apiBaseRaw = (apiMeta?.content || "").trim().replace(/\/+$/, "");
  const isMixedContentBlocked =
    window.location.protocol === "https:" && /^http:\/\//i.test(apiBaseRaw);
  const apiBase = isMixedContentBlocked ? "" : apiBaseRaw;
  const buildApiUrl = (path) => (apiBase ? `${apiBase}${path}` : path);

  const showMessage = (text) => {
    if (window.Telegram?.WebApp?.showAlert) {
      window.Telegram.WebApp.showAlert(text);
      return;
    }
    window.alert(text);
  };

  const formatRub = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${Number.isInteger(numeric) ? numeric : numeric.toFixed(2)}₽`;
  };

  const formatUsd = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${numeric.toFixed(2)}$`;
  };

  const getFixedStarRateFromProfilePerks = () => {
    const numeric = Number(window.starslixProfilePerks?.fixedStarRateRub);
    return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
  };

  const resolveStarsOffer = (offer) => {
    const amount = Number(offer?.amount);
    if (!Number.isFinite(amount) || amount <= 0) {
      return offer;
    }
    const fixedRate = getFixedStarRateFromProfilePerks();
    if (fixedRate === null) {
      return offer;
    }
    const usdRubRate = Number(window.starslixMiniappConfig?.usdRubRate);
    const safeUsdRubRate = Number.isFinite(usdRubRate) && usdRubRate > 0 ? usdRubRate : 76.5;
    const rub = Math.round(amount * fixedRate * 100) / 100;
    const usd = Math.round((rub / safeUsdRubRate) * 100) / 100;
    return {
      ...(offer || {}),
      amount,
      rub,
      usd,
    };
  };

  const syncPanelPriceRow = (gridNode) => {
    const panelView = gridNode?.closest(".panel-view");
    if (!panelView) {
      return;
    }
    const priceUsdNode = panelView.querySelector("[data-price-usd]");
    const priceRubNode = panelView.querySelector("[data-price-rub]");
    if (!priceUsdNode || !priceRubNode) {
      return;
    }

    const sourceCard =
      gridNode.querySelector(".offer-card.is-active");
    if (!sourceCard) {
      priceUsdNode.textContent = "—";
      priceRubNode.textContent = "—";
      return;
    }

    const rawPrice = sourceCard.querySelector(".offer-card-price")?.textContent?.trim() || "";
    const [usdPart = "—", rubPart = "—"] = rawPrice.split("/");
    priceUsdNode.textContent = usdPart.trim() || "—";
    priceRubNode.textContent = rubPart.trim() || "—";
  };

  const applyConfigToCards = (config) => {
    window.starslixMiniappConfig = {
      ...(window.starslixMiniappConfig || {}),
      ...(config || {}),
    };

    const starsCards = Array.from(document.querySelectorAll(".offer-grid-stars .offer-card"));
    (config?.stars || []).slice(0, starsCards.length).forEach((offer, index) => {
      const card = starsCards[index];
      const normalizedOffer = resolveStarsOffer(offer);
      const amountNode = card.querySelector(".offer-card-amount");
      const priceNode = card.querySelector(".offer-card-price");
      if (amountNode) {
        amountNode.textContent = String(normalizedOffer.amount);
      }
      if (priceNode) {
        priceNode.textContent = `${formatUsd(normalizedOffer.usd)}/${formatRub(normalizedOffer.rub)}`;
      }
      card.dataset.amountValue = String(normalizedOffer.amount);
    });

    const premiumCards = Array.from(document.querySelectorAll(".offer-grid-premium .offer-card"));
    (config?.premium || []).slice(0, premiumCards.length).forEach((offer, index) => {
      const card = premiumCards[index];
      const priceNode = card.querySelector(".offer-card-price");
      if (priceNode) {
        priceNode.textContent = `${formatUsd(offer.usd)}/${formatRub(offer.rub)}`;
      }
      card.dataset.amountValue = String(offer.months);
    });

    document.querySelectorAll(".offer-grid").forEach(syncPanelPriceRow);
    window.dispatchEvent(new CustomEvent("starslix:pricing-updated"));
    window.dispatchEvent(
      new CustomEvent("starslix:config-updated", {
        detail: {
          config: window.starslixMiniappConfig || {},
        },
      })
    );
  };

  const fetchMiniappConfig = async () => {
    try {
      const response = await fetch(buildApiUrl("/api/miniapp/config"), {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      });
      if (!response.ok) {
        return;
      }

      const data = await response.json();
      if (data?.ok) {
        applyConfigToCards(data);
      }
    } catch (error) {}
  };

  let configRefreshInFlight = false;
  const refreshMiniappConfig = async () => {
    if (document.hidden || configRefreshInFlight) {
      return;
    }
    configRefreshInFlight = true;
    try {
      await fetchMiniappConfig();
    } finally {
      configRefreshInFlight = false;
    }
  };

  const mergeTotalsFromServerEvent = (payload) => {
    const totals = payload?.totals;
    if (!totals || typeof totals !== "object") {
      return;
    }
    const totalStars = Number(totals.totalStars);
    const totalPremiumMonths = Number(totals.totalPremiumMonths);
    window.starslixMiniappConfig = {
      ...(window.starslixMiniappConfig || {}),
      totals: {
        ...(window.starslixMiniappConfig?.totals || {}),
        ...(Number.isFinite(totalStars) ? { totalStars } : {}),
        ...(Number.isFinite(totalPremiumMonths) ? { totalPremiumMonths } : {}),
      },
      ...(Number.isFinite(totalStars) ? { totalStars } : {}),
      ...(Number.isFinite(totalPremiumMonths) ? { totalPremiumMonths } : {}),
    };
    window.dispatchEvent(
      new CustomEvent("starslix:config-updated", {
        detail: {
          config: window.starslixMiniappConfig || {},
        },
      })
    );
  };

  const parseEventPayload = (event) => {
    try {
      return JSON.parse(String(event?.data || "{}"));
    } catch {
      return {};
    }
  };

  let eventsSource = null;
  let eventsReconnectTimer = null;
  let eventsConnectionStarted = false;

  const closeMiniappEvents = () => {
    if (eventsSource) {
      try {
        eventsSource.close();
      } catch {}
      eventsSource = null;
    }
    if (eventsReconnectTimer) {
      window.clearTimeout(eventsReconnectTimer);
      eventsReconnectTimer = null;
    }
  };

  const scheduleEventsReconnect = () => {
    if (eventsReconnectTimer) {
      return;
    }
    eventsReconnectTimer = window.setTimeout(() => {
      eventsReconnectTimer = null;
      connectMiniappEvents();
    }, 1500);
  };

  const connectMiniappEvents = () => {
    if (eventsSource || eventsConnectionStarted) {
      return;
    }
    if (document.hidden) {
      return;
    }
    const initData = String(window.Telegram?.WebApp?.initData || "").trim();
    if (!initData || typeof window.EventSource !== "function") {
      return;
    }

    const separator = buildApiUrl("/api/miniapp/events").includes("?") ? "&" : "?";
    const eventsUrl = `${buildApiUrl("/api/miniapp/events")}${separator}initData=${encodeURIComponent(initData)}`;
    eventsConnectionStarted = true;
    try {
      eventsSource = new EventSource(eventsUrl);
    } catch {
      eventsConnectionStarted = false;
      scheduleEventsReconnect();
      return;
    }

    eventsSource.addEventListener("ready", (event) => {
      mergeTotalsFromServerEvent(parseEventPayload(event));
    });

    eventsSource.addEventListener("purchase_finalized", (event) => {
      const payload = parseEventPayload(event);
      mergeTotalsFromServerEvent(payload);
      window.dispatchEvent(
        new CustomEvent("starslix:server-update", {
          detail: payload || {},
        })
      );
    });

    eventsSource.addEventListener("reviews_updated", () => {
      window.dispatchEvent(
        new CustomEvent("starslix:reviews-updated")
      );
    });

    eventsSource.addEventListener("support_update", (event) => {
      const payload = parseEventPayload(event);
      window.dispatchEvent(
        new CustomEvent("starslix:support-updated", {
          detail: payload || {},
        })
      );
    });

    eventsSource.onerror = () => {
      closeMiniappEvents();
      eventsConnectionStarted = false;
      scheduleEventsReconnect();
    };

    eventsSource.onopen = () => {
      eventsConnectionStarted = false;
    };
  };

  const clearPanelInputs = (panelView) => {
    if (!panelView) {
      return;
    }

    panelView.querySelectorAll(".form-input").forEach((inputNode) => {
      if (!(inputNode instanceof HTMLInputElement || inputNode instanceof HTMLTextAreaElement)) {
        return;
      }
      inputNode.value = "";
      inputNode.dispatchEvent(new Event("input", { bubbles: true }));
      inputNode.dispatchEvent(new Event("change", { bubbles: true }));
    });

    const activeElement = document.activeElement;
    if (activeElement instanceof HTMLElement && panelView.contains(activeElement)) {
      activeElement.blur();
    }
  };

  const parseAmountFromCard = (card) => {
    if (!card) {
      return "";
    }
    if (card.dataset.amountValue) {
      return card.dataset.amountValue;
    }
    const amountText = card.querySelector(".offer-card-amount")?.textContent?.trim() || "";
    return amountText.replace(/[^0-9.]/g, "");
  };

  const getOrderPayload = (panelView) => {
    const type = panelView?.dataset?.view;
    if (type !== "stars" && type !== "premium") {
      return null;
    }

    const usernameInput = panelView.querySelector(".form-input--telegram");
    const promoInput = panelView.querySelector(".form-input--promo");
    const username = usernameInput?.value?.trim() || "";
    const promoCode = promoInput?.value?.trim() || "";

    let amount = "";
    if (type === "stars") {
      const amountInput = panelView.querySelector('.form-input[type="number"]');
      const selectedCard =
        panelView.querySelector(".offer-card.is-active") ||
        panelView.querySelector(".offer-grid-stars .offer-card");
      amount = (amountInput?.value || "").trim() || parseAmountFromCard(selectedCard);
    } else {
      const selectedCard =
        panelView.querySelector(".offer-card.is-active") ||
        panelView.querySelector(".offer-grid-premium .offer-card");
      amount = parseAmountFromCard(selectedCard);
    }

    if (!username) {
      return { error: "Введите Telegram username" };
    }
    if (!amount) {
      return { error: type === "stars" ? "Введите количество звёзд" : "Выберите пакет Premium" };
    }

    const initData = window.Telegram?.WebApp?.initData || "";
    const hasSendData = typeof window.Telegram?.WebApp?.sendData === "function";
    // Even with empty meta base we can call same-origin `/api/miniapp/...`.
    const shouldUseApi = true;
    if (shouldUseApi && !initData) {
      return { error: "Открой мини-приложение через Telegram" };
    }
    if (!shouldUseApi && !initData && !hasSendData) {
      return { error: "Открой мини-приложение через Telegram" };
    }

    return {
      type,
      username,
      amount,
      promoCode,
      initData,
    };
  };

  const submitOrder = async (button, panelView) => {
    const payload = getOrderPayload(panelView);
    if (!payload || payload.error) {
      showMessage(payload?.error || "Некорректные данные заказа");
      return;
    }

    button.disabled = true;
    button.classList.add("is-loading");

    try {
      const response = await fetch(buildApiUrl("/api/miniapp/order"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(payload),
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok || !data?.ok) {
        const details = data?.error || `Ошибка API (${response.status})`;
        showMessage(`Не удалось отправить заявку: ${details}`);
        return;
      }

      if (typeof window.starslixAddCurrentOrderToHistory === "function") {
        const promoErrorValue = String(data?.promo?.error || "").trim();
        const promoCodeValue = String(data?.promo?.code || payload.promoCode || "")
          .trim()
          .toUpperCase();
        const operationData = data?.operation && typeof data.operation === "object" ? data.operation : {};
        window.starslixAddCurrentOrderToHistory({
          type: payload.type,
          amount: payload.amount,
          priceUsd: data?.price?.usd,
          priceRub: data?.price?.rub,
          promoCode: promoCodeValue,
          promoDiscount: data?.promo?.discount,
          promoError: promoErrorValue,
          status: "pending",
          operationId: operationData.id || "",
          operationSource: operationData.source || "miniapp",
          operationNumericId: operationData.historyId,
          buyerUserId: operationData.buyerUserId,
          buyerUsername: operationData.buyerUsername,
          buyerFullName: operationData.buyerFullName,
          targetUsername: operationData.targetUsername || payload.username,
          channel: operationData.channel || "miniapp_test",
          createdAt: operationData.createdAt || new Date().toISOString(),
        });
      }

      clearPanelInputs(panelView);
      const promoErrorNotice = data?.promo?.error ? `\nПромокод: ${data.promo.error}` : "";
      showMessage(`Заявка отправлена в группу${promoErrorNotice}`);
      return;
    } catch (error) {
      showMessage(
        `Ошибка сети при отправке заявки: ${error?.message || "unknown error"}`
      );
    } finally {
      button.disabled = false;
      button.classList.remove("is-loading");
    }
  };

  document
    .querySelectorAll(".form-stack--stars .order-submit-button, .form-stack--premium .order-submit-button")
    .forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        const panelView = button.closest(".panel-view");
        if (!panelView) {
          return;
        }
        submitOrder(button, panelView);
      });
    });

  document.querySelectorAll(".form-stack--ton .order-submit-button").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      const panelView = button.closest(".panel-view");
      clearPanelInputs(panelView);
    });
  });

  window.addEventListener("starslix:screen-changed", (event) => {
    const screen = String(event?.detail?.screen || "");
    if (screen === "home" || screen === "profile") {
      refreshMiniappConfig();
      connectMiniappEvents();
    }
  });

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      refreshMiniappConfig();
      connectMiniappEvents();
    } else {
      closeMiniappEvents();
      eventsConnectionStarted = false;
    }
  });
  window.addEventListener("starslix:profile-perks-updated", () => {
    applyConfigToCards(window.starslixMiniappConfig || {});
  });

  refreshMiniappConfig();
  connectMiniappEvents();
})();

(() => {
  const languageToggleButton = document.getElementById("languageToggleButton");
  const languageDropdown = document.getElementById("languageDropdown");
  const languageSwitch = document.getElementById("headerLanguageSwitch");
  const languageOptions = Array.from(document.querySelectorAll(".header-language-option"));
  if (!languageToggleButton || !languageDropdown || !languageSwitch || !languageOptions.length) {
    return;
  }

  const translations = {
    ru: {
      loaderAria: "Загрузка",
      profileButtonAria: "Открыть страницу профиля",
      telegramButton: "Telegram-бот",
      languageButton: "Смена языка",
      languageShort: "RU",
      languageOptionRu: "Русский",
      languageOptionEn: "Английский",
      heroMobileLine1: "Telegram Premium",
      heroMobileLine2Prefix: "и ",
      heroStars: "Stars",
      heroMobileLine3: "официальной цены",
      heroWideLine1: "Telegram Premium",
      heroWideLine2Suffix: "дешевле",
      heroSubtitleLine1: "Мгновенная активация • Без KYC • Оплата через СБП",
      heroSubtitleLine2: "",
      topPillAria: "Улучшите свои впечатления от Telegram",
      topPillText: "Улучшите свои впечатления от Telegram",
      dockAria: "Нижнее меню",
      dockHome: "Главная",
      dockLeaders: "Лидеры",
      dockContest: "Конкурс",
      dockProfile: "Профиль",
      leadersPageAria: "Страница лидеров",
      leadersTitle: "Лидеры",
      leadersText: "Раздел лидеров в разработке.",
      homeReviewsAria: "Отзывы пользователей",
      homeReviewsTitle: "Отзывы",
      homeReviewsRefreshAria: "Обновить отзывы",
      homeReviewsLoading: "Загружаем отзывы...",
      homeReviewsEmpty: "Отзывов пока нет",
      homeReviewsError: "Не удалось загрузить отзывы",
      contestPageAria: "Страница конкурса",
      contestTitle: "Конкурс",
      contestText: "Раздел конкурса в разработке.",
      mainPanelAria: "Основной блок контента",
      panelTabsAria: "Разделы",
      panelTabStarsAria: "Звёзды",
      panelTabPremiumAria: "Премиум",
      starsGridAria: "Пакеты звёзд",
      premiumGridAria: "Пакеты премиума",
      cardStars100Aria: "Пакет звёзд 100",
      cardStars500Aria: "Пакет звёзд 500",
      cardStars1000Aria: "Пакет звёзд 1000",
      cardPremium100Aria: "Пакет премиума 3 месяца",
      cardPremium500Aria: "Пакет премиума 6 месяцев",
      cardPremium1000Aria: "Пакет премиума 12 месяцев",
      premiumDuration3: "3 мес.",
      premiumDuration6: "6 мес.",
      premiumDuration12: "12 мес.",
      badgeValue: "Выгодно",
      badgePopular: "Популярно",
      usernameFloatingLabel: "Введите Telegram @username",
      selfFillButton: "Себе",
      amountFloatingLabel: "Введите количество (50 - 10000)",
      tonAmountFloatingLabel: "Введите количество TON",
      promoFloatingLabel: "Введите промокод",
      paymentMethodLabel: "Метод оплаты",
      paymentMethodText: "СПБ #1 (PALLY)",
      orderButton: "Оформить заказ",
      tonOrderButton: "Отправить TON",
      orderNote:
        "Приобретая Telegram Stars или Telegram Premium, вы подтверждаете свое согласие с нашими Условиями и подтверждаете, что не являетесь резидентом США.",
      profilePageAria: "Страница профиля",
      profileCloseAria: "Закрыть профиль",
      profileFallbackName: "Пользователь Telegram",
      profileLabelUsername: "Telegram username",
      profileLabelTelegramId: "Telegram ID",
      profileStatPurchasesLabel: "Покупок",
      profileStatSpentLabel: "Потрачено",
      profileStatPromosLabel: "Промокоды",
      profileProgressTitle: "Прогресс",
      profileAdminTitle: "Права администратора",
      profileAdminStatus: "Доступ к управлению miniapp",
      profileHistoryTitle: "История покупок",
      profileHistoryEmpty: "Покупок пока нет",
      profileHistoryEmptyAction: "Сделать первую покупку",
      profileHistoryFilterAll: "Все",
      profileHistoryFilterStars: "Звёзды",
      profileHistoryFilterPremium: "Premium",
      profileHistoryFilterTon: "TON",
      profileHistorySearchPlaceholder: "Поиск по сумме или дате",
      profileAdminButton: "Админ-панель",
    },
    en: {
      loaderAria: "Loading",
      profileButtonAria: "Open profile page",
      telegramButton: "Telegram bot",
      languageButton: "Switch language",
      languageShort: "EN",
      languageOptionRu: "Russian",
      languageOptionEn: "English",
      heroMobileLine1: "Telegram Premium",
      heroMobileLine2Prefix: "and ",
      heroStars: "Stars",
      heroMobileLine3: "official price",
      heroWideLine1: "Telegram Premium",
      heroWideLine2Suffix: "below",
      heroSubtitleLine1: "Instant activation • No KYC • Pay via SBP",
      heroSubtitleLine2: "",
      topPillAria: "Enhance your Telegram experience",
      topPillText: "Enhance your Telegram experience",
      dockAria: "Bottom menu",
      dockHome: "Home",
      dockLeaders: "Leaders",
      dockContest: "Contest",
      dockProfile: "Profile",
      leadersPageAria: "Leaders page",
      leadersTitle: "Leaders",
      leadersText: "Leaders section is in progress.",
      homeReviewsAria: "User reviews",
      homeReviewsTitle: "Reviews",
      homeReviewsRefreshAria: "Refresh reviews",
      homeReviewsLoading: "Loading reviews...",
      homeReviewsEmpty: "No reviews yet",
      homeReviewsError: "Failed to load reviews",
      contestPageAria: "Contest page",
      contestTitle: "Contest",
      contestText: "Contest section is in progress.",
      mainPanelAria: "Main content section",
      panelTabsAria: "Sections",
      panelTabStarsAria: "Stars",
      panelTabPremiumAria: "Premium",
      starsGridAria: "Star packages",
      premiumGridAria: "Premium packages",
      cardStars100Aria: "100 stars package",
      cardStars500Aria: "500 stars package",
      cardStars1000Aria: "1000 stars package",
      cardPremium100Aria: "Premium package 3 months",
      cardPremium500Aria: "Premium package 6 months",
      cardPremium1000Aria: "Premium package 12 months",
      premiumDuration3: "3 mo.",
      premiumDuration6: "6 mo.",
      premiumDuration12: "12 mo.",
      badgeValue: "Best value",
      badgePopular: "Popular",
      usernameFloatingLabel: "Enter Telegram @username",
      selfFillButton: "Me",
      amountFloatingLabel: "Enter amount (50 - 10000)",
      tonAmountFloatingLabel: "Enter TON amount",
      promoFloatingLabel: "Enter promo code",
      paymentMethodLabel: "Payment method",
      paymentMethodText: "SBP #1 (PALLY)",
      orderButton: "Place order",
      tonOrderButton: "Send TON",
      orderNote:
        "By purchasing Telegram Stars or Telegram Premium, you confirm your agreement with our Terms and confirm that you are not a resident of the USA.",
      profilePageAria: "Profile page",
      profileCloseAria: "Close profile",
      profileFallbackName: "Telegram user",
      profileLabelUsername: "Telegram username",
      profileLabelTelegramId: "Telegram ID",
      profileStatPurchasesLabel: "Purchases",
      profileStatSpentLabel: "Spent",
      profileStatPromosLabel: "Promos",
      profileProgressTitle: "Progress",
      profileAdminTitle: "Administrator access",
      profileAdminStatus: "Access to miniapp management",
      profileHistoryTitle: "Purchase history",
      profileHistoryEmpty: "No purchases yet",
      profileHistoryEmptyAction: "Make first purchase",
      profileHistoryFilterAll: "All",
      profileHistoryFilterStars: "Stars",
      profileHistoryFilterPremium: "Premium",
      profileHistoryFilterTon: "TON",
      profileHistorySearchPlaceholder: "Search by amount or date",
      profileAdminButton: "Admin panel",
    },
  };

  const storageKey = "starslix_lang";

  const setText = (selector, value) => {
    const element = document.querySelector(selector);
    if (element) {
      element.textContent = value;
    }
  };

  const setAttr = (selector, attr, value) => {
    const element = document.querySelector(selector);
    if (element) {
      element.setAttribute(attr, value);
    }
  };

  const setPremiumHeroLine = (selector, value) => {
    const element = document.querySelector(selector);
    if (!element) {
      return;
    }
    const safeText = String(value ?? "");
    element.innerHTML = safeText.replace(
      /Premium/gi,
      (match) => `<span class="hero-title-premium">${match}</span>`
    );
  };

  const setHeroSecondLines = (dict) => {
    const secondLine = `${dict.heroMobileLine2Prefix}<span class="hero-title-accent">${dict.heroStars}</span> ${dict.heroWideLine2Suffix}`;
    const mobileLine2 = document.querySelector(
      ".hero-title-layout-mobile .hero-title-line:nth-child(2)"
    );
    if (mobileLine2) {
      mobileLine2.innerHTML = secondLine;
    }

    const wideLine2 = document.querySelector(
      ".hero-title-layout-wide .hero-title-line:nth-child(2)"
    );
    if (wideLine2) {
      wideLine2.innerHTML = secondLine;
    }
  };

  const applyLanguage = (lang) => {
    const dict = translations[lang] || translations.ru;

    document.documentElement.lang = lang;
    setAttr("#pageLoader", "aria-label", dict.loaderAria);
    setAttr("#userAvatarToggle", "aria-label", dict.profileButtonAria);

    setText("#telegramBotButton", dict.telegramButton);
    setAttr("#languageToggleButton", "aria-label", dict.languageButton);
    setText("#languageToggleValue", dict.languageShort);
    setText("#languageOptionRu", dict.languageOptionRu);
    setText("#languageOptionEn", dict.languageOptionEn);

    languageOptions.forEach((option) => {
      option.classList.toggle("is-active", option.dataset.lang === lang);
    });

    setPremiumHeroLine(".hero-title-layout-mobile .hero-title-line:nth-child(1)", dict.heroMobileLine1);
    setHeroSecondLines(dict);
    setText(".hero-title-layout-mobile .hero-title-line:nth-child(3)", dict.heroMobileLine3);
    setPremiumHeroLine(".hero-title-layout-wide .hero-title-line:nth-child(1)", dict.heroWideLine1);
    setText(".hero-title-layout-wide .hero-title-line:nth-child(3)", dict.heroMobileLine3);

    setText(".hero-subtitle-line:nth-child(1)", dict.heroSubtitleLine1);
    setText(".hero-subtitle-line:nth-child(2)", dict.heroSubtitleLine2);
    setAttr("#topPill", "aria-label", dict.topPillAria);
    setText("#topPillText", dict.topPillText);
    setAttr("#appDock", "aria-label", dict.dockAria);
    setText("#dockLabelHome", dict.dockHome);
    setText('[data-dock-label-tint="home"]', dict.dockHome);
    setText("#dockLabelLeaders", dict.dockLeaders);
    setText('[data-dock-label-tint="leaders"]', dict.dockLeaders);
    setText("#dockLabelContest", dict.dockContest);
    setText('[data-dock-label-tint="contest"]', dict.dockContest);
    setText("#dockLabelProfile", dict.dockProfile);
    setText('[data-dock-label-tint="profile"]', dict.dockProfile);
    setAttr("#leadersPage", "aria-label", dict.leadersPageAria);
    setText("#leadersTitle", dict.leadersTitle);
    setText("#leadersText", dict.leadersText);
    setAttr("#homeReviews", "aria-label", dict.homeReviewsAria);
    setText("#homeReviewsTitle", dict.homeReviewsTitle);
    setText("#homeReviewsEmpty", dict.homeReviewsLoading);
    setAttr("#contestPage", "aria-label", dict.contestPageAria);
    setText("#contestTitle", dict.contestTitle);
    setText("#contestText", dict.contestText);

    setAttr(".main-glass-panel", "aria-label", dict.mainPanelAria);
    setAttr(".panel-tabs", "aria-label", dict.panelTabsAria);
    setAttr('.panel-tab[data-panel="stars"]', "aria-label", dict.panelTabStarsAria);
    setAttr('.panel-tab[data-panel="premium"]', "aria-label", dict.panelTabPremiumAria);

    setAttr(".offer-grid-stars", "aria-label", dict.starsGridAria);
    setAttr(".offer-grid-premium", "aria-label", dict.premiumGridAria);
    setAttr(".offer-grid-stars .offer-card:nth-child(1)", "aria-label", dict.cardStars100Aria);
    setAttr(".offer-grid-stars .offer-card:nth-child(2)", "aria-label", dict.cardStars500Aria);
    setAttr(".offer-grid-stars .offer-card:nth-child(3)", "aria-label", dict.cardStars1000Aria);
    setAttr(".offer-grid-premium .offer-card:nth-child(1)", "aria-label", dict.cardPremium100Aria);
    setAttr(".offer-grid-premium .offer-card:nth-child(2)", "aria-label", dict.cardPremium500Aria);
    setAttr(".offer-grid-premium .offer-card:nth-child(3)", "aria-label", dict.cardPremium1000Aria);
    setText(".offer-grid-premium .offer-card:nth-child(1) .offer-card-amount", dict.premiumDuration3);
    setText(".offer-grid-premium .offer-card:nth-child(2) .offer-card-amount", dict.premiumDuration6);
    setText(".offer-grid-premium .offer-card:nth-child(3) .offer-card-amount", dict.premiumDuration12);

    document.querySelectorAll(".offer-card--value .offer-card-badge").forEach((badge) => {
      badge.textContent = dict.badgeValue;
    });
    document.querySelectorAll(".offer-card--popular .offer-card-badge").forEach((badge) => {
      badge.textContent = dict.badgePopular;
    });

    setText(
      ".form-stack--stars .form-field:nth-child(1) .form-floating-label",
      dict.usernameFloatingLabel
    );
    setText(
      ".form-stack--stars .form-field:nth-child(2) .form-floating-label",
      dict.amountFloatingLabel
    );
    setText(
      ".form-stack--stars .form-field:nth-child(3) .form-floating-label",
      dict.promoFloatingLabel
    );
    setText(
      ".form-stack--stars .form-field:nth-child(4) .form-field-label",
      dict.paymentMethodLabel
    );
    setText(
      ".form-stack--premium .form-field:nth-child(1) .form-floating-label",
      dict.usernameFloatingLabel
    );
    setText(
      ".form-stack--premium .form-field:nth-child(2) .form-floating-label",
      dict.promoFloatingLabel
    );
    setText(
      ".form-stack--premium .form-field:nth-child(3) .form-field-label",
      dict.paymentMethodLabel
    );
    setText(
      ".form-stack--ton .form-field:nth-child(1) .form-floating-label",
      dict.usernameFloatingLabel
    );
    setText(
      ".form-stack--ton .form-field:nth-child(2) .form-floating-label",
      dict.tonAmountFloatingLabel
    );
    setText(
      ".form-stack--ton .form-field:nth-child(3) .form-field-label",
      dict.paymentMethodLabel
    );

    setAttr("#profilePage", "aria-label", dict.profilePageAria);
    setAttr("#profileCloseButton", "aria-label", dict.profileCloseAria);
    setText("#profileLabelUsername", dict.profileLabelUsername);
    setText("#profileLabelId", dict.profileLabelTelegramId);
    setText("#profileStatPurchasesLabel", dict.profileStatPurchasesLabel);
    setText("#profileStatSpentLabel", dict.profileStatSpentLabel);
    setText("#profileStatPromosLabel", dict.profileStatPromosLabel);
    setText("#profileProgressTitle", dict.profileProgressTitle);
    setText("#profileAdminTitle", dict.profileAdminTitle);
    setText("#profileAdminStatus", dict.profileAdminStatus);
    setText("#profileHistoryTitle", dict.profileHistoryTitle);
    setText("#profileHistoryEmpty .profile-history-empty", dict.profileHistoryEmpty);
    setText("#profileHistoryEmptyAction", dict.profileHistoryEmptyAction);
    setText("#profileHistoryFilterAll", dict.profileHistoryFilterAll);
    setText("#profileHistoryFilterStars", dict.profileHistoryFilterStars);
    setText("#profileHistoryFilterPremium", dict.profileHistoryFilterPremium);
    setText("#profileHistoryFilterTon", dict.profileHistoryFilterTon);
    setAttr("#profileHistorySearchInput", "placeholder", dict.profileHistorySearchPlaceholder);
    setText("#profileAdminButton", dict.profileAdminButton);
    document.querySelectorAll(".form-self-fill-button").forEach((node) => {
      node.textContent = dict.selfFillButton;
    });

    const profileFullname = document.getElementById("profileFullname");
    if (profileFullname?.dataset.empty === "true") {
      profileFullname.textContent = dict.profileFallbackName;
    }

    document.querySelectorAll(".form-static-text").forEach((node) => {
      node.textContent = dict.paymentMethodText;
    });
    document
      .querySelectorAll(".form-stack--stars .order-submit-button, .form-stack--premium .order-submit-button")
      .forEach((node) => {
      node.textContent = dict.orderButton;
    });
    setText(".form-stack--ton .order-submit-button", dict.tonOrderButton);
    document.querySelectorAll(".order-note").forEach((node) => {
      node.textContent = dict.orderNote;
    });

    window.dispatchEvent(new CustomEvent("starslix:language-updated"));
    window.dispatchEvent(new CustomEvent("starslix:dock-labels-updated"));
  };

  const getSavedLanguage = () => {
    try {
      const savedLanguage = window.localStorage.getItem(storageKey);
      return savedLanguage === "en" || savedLanguage === "ru" ? savedLanguage : null;
    } catch {
      return null;
    }
  };

  const saveLanguage = (lang) => {
    try {
      window.localStorage.setItem(storageKey, lang);
    } catch {
      // ignore write errors
    }
  };

  let currentLanguage = getSavedLanguage() || "ru";
  applyLanguage(currentLanguage);

  let isDropdownOpen = false;

  const setDropdownState = (open) => {
    isDropdownOpen = open;
    languageDropdown.classList.toggle("is-open", open);
    languageDropdown.setAttribute("aria-hidden", open ? "false" : "true");
    languageToggleButton.setAttribute("aria-expanded", open ? "true" : "false");
  };

  languageToggleButton.addEventListener("click", (event) => {
    event.stopPropagation();
    setDropdownState(!isDropdownOpen);
  });

  languageOptions.forEach((option) => {
    option.addEventListener("click", (event) => {
      event.stopPropagation();
      const nextLanguage = option.dataset.lang;
      if (nextLanguage !== "ru" && nextLanguage !== "en") {
        return;
      }

      currentLanguage = nextLanguage;
      applyLanguage(currentLanguage);
      saveLanguage(currentLanguage);
      setDropdownState(false);
    });
  });

  document.addEventListener("pointerdown", (event) => {
    if (!isDropdownOpen) {
      return;
    }

    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }

    if (target.closest("#headerLanguageSwitch")) {
      return;
    }

    setDropdownState(false);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && isDropdownOpen) {
      setDropdownState(false);
    }
  });
})();

(() => {
  const adminHubPage = document.getElementById("adminHubPage");
  const adminSectionPage = document.getElementById("adminSectionPage");
  const adminHubCloseButton = document.getElementById("adminHubCloseButton");
  const adminSectionCloseButton = document.getElementById("adminSectionCloseButton");
  const adminSectionContent = document.getElementById("adminSectionContent");
  const adminSectionTitle = document.getElementById("adminSectionTitle");
  const adminSectionSubtitle = document.getElementById("adminSectionSubtitle");
  const sectionButtons = Array.from(document.querySelectorAll("[data-admin-section]"));
  if (
    !adminHubPage ||
    !adminSectionPage ||
    !adminSectionContent ||
    !adminSectionTitle ||
    !adminSectionSubtitle ||
    !sectionButtons.length
  ) {
    return;
  }

  const apiMeta = document.querySelector('meta[name="starslix-api-base"]');
  const apiBaseRaw = (apiMeta?.content || "").trim().replace(/\/+$/, "");
  const isMixedContentBlocked =
    window.location.protocol === "https:" && /^http:\/\//i.test(apiBaseRaw);
  const apiBase = isMixedContentBlocked ? "" : apiBaseRaw;
  const buildApiUrl = (path) => (apiBase ? `${apiBase}${path}` : path);

  const getInitData = () => String(window.Telegram?.WebApp?.initData || "").trim();
  const navigateToScreen = (screen) => {
    window.dispatchEvent(
      new CustomEvent("starslix:navigate", {
        detail: { screen },
      })
    );
  };

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const formatNumber = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "0";
    }
    return numeric.toLocaleString("ru-RU");
  };

  const formatMoney = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "0.00";
    }
    return numeric.toFixed(2);
  };

  const copyTextToClipboard = async (rawValue) => {
    const text = String(rawValue || "").trim();
    if (!text || text === "—") {
      return false;
    }
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
        return true;
      }
    } catch {}
    try {
      const helper = document.createElement("textarea");
      helper.value = text;
      helper.setAttribute("readonly", "readonly");
      helper.style.position = "fixed";
      helper.style.opacity = "0";
      helper.style.pointerEvents = "none";
      document.body.appendChild(helper);
      helper.select();
      const copied = document.execCommand("copy");
      helper.remove();
      return Boolean(copied);
    } catch {
      return false;
    }
  };

  const toIsoDate = (date) => {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  };

  const stripDataUrlPrefix = (value) => {
    const text = String(value || "");
    const commaIndex = text.indexOf(",");
    return commaIndex >= 0 ? text.slice(commaIndex + 1) : text;
  };

  const fileToDataUrl = (file) =>
    new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ""));
      reader.onerror = () => reject(new Error("Не удалось прочитать файл"));
      reader.readAsDataURL(file);
    });

  const parseButtonsText = (rawValue) => {
    const lines = String(rawValue || "")
      .split(/\r?\n/g)
      .map((line) => line.trim())
      .filter(Boolean);

    const buttons = [];
    for (const line of lines) {
      const separatorIndex = line.indexOf(" - ");
      if (separatorIndex <= 0) {
        throw new Error("Формат кнопок: Текст - https://ссылка");
      }
      const text = line.slice(0, separatorIndex).trim();
      const url = line.slice(separatorIndex + 3).trim();
      if (!text || !url || !/^(https?:\/\/|tg:\/\/)/i.test(url)) {
        throw new Error("Проверьте кнопки: ссылка должна начинаться с http://, https:// или tg://");
      }
      buttons.push({ text, url });
    }
    return buttons;
  };

  const showAlert = (text) => {
    if (window.Telegram?.WebApp?.showAlert) {
      window.Telegram.WebApp.showAlert(text);
      return;
    }
    window.alert(text);
  };

  const adminRequest = async (action, payload = {}) => {
    const initData = getInitData();
    if (!initData) {
      throw new Error("Открой мини-приложение через Telegram");
    }

    const response = await fetch(buildApiUrl("/api/miniapp/admin/action"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({
        initData,
        action,
        payload,
      }),
    });

    const rawText = await response.text().catch(() => "");
    let data = {};
    try {
      data = rawText ? JSON.parse(rawText) : {};
    } catch {
      data = {};
    }
    if (!response.ok || !data?.ok) {
      if (response.status === 413) {
        throw new Error("Файл слишком большой, загрузите фото поменьше");
      }
      throw new Error(data?.error || "Не удалось выполнить действие");
    }
    return data?.data || {};
  };

  const requestAdminRates = async (starRates = null) => {
    const initData = getInitData();
    if (!initData) {
      throw new Error("Открой мини-приложение через Telegram");
    }

    const body = { initData };
    if (starRates) {
      body.starRates = starRates;
    }

    const response = await fetch(buildApiUrl("/api/miniapp/admin/rates"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify(body),
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok || !data?.ok) {
      throw new Error(data?.error || "Не удалось получить тарифы");
    }
    return data;
  };

  const getSectionTitleByKey = (sectionKey) =>
    sectionButtons.find((button) => button.dataset.adminSection === sectionKey)?.textContent?.trim() ||
    "Раздел";

  const sectionSubtitles = {
    stats: "Пользователи, продажи и очистка",
    promos: "Создание, активные и удаление",
    referrals: "Показ и сброс активных рефералов",
    stars: "Проданные звёзды и тарифы",
    payment: "Баланс, история и выплаты",
    broadcast: "Текст, фото и кнопки ссылок",
    "find-user": "Поиск по username или ID",
    "add-stars": "Ручное начисление звёзд",
    admins: "Добавление и удаление админов",
    support: "Чаты пользователей, ответы, переименование и удаление",
    logs: "Кто, что и когда делал",
    operations: "Поиск операций по номеру и данным пользователя",
  };

  // Section renderers are defined below.
  let sectionRenderers = {};

  const openSection = async (sectionKey) => {
    adminSectionTitle.textContent = getSectionTitleByKey(sectionKey);
    adminSectionSubtitle.textContent = sectionSubtitles[sectionKey] || "Выберите действие";
    adminSectionContent.innerHTML = "";
    navigateToScreen("admin-section");

    const renderer = sectionRenderers[sectionKey];
    if (typeof renderer === "function") {
      await renderer();
      return;
    }
    adminSectionContent.innerHTML = '<div class="admin-result-card">Раздел в разработке.</div>';
  };

  const renderStatsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-panel-actions">
        <button class="admin-action-button" id="adminStatsUsersButton" type="button">Пользователи</button>
        <button class="admin-action-button" id="adminStatsSalesButton" type="button">Продажи</button>
        <button class="admin-action-button" id="adminStatsClearButton" type="button">Очистить статистику</button>
      </div>
      <div class="admin-split admin-split--search" id="adminStatsUsersSearchRow" hidden>
        <div class="admin-search-wrap">
          <input class="admin-input admin-input--search" id="adminStatsUsersSearchInput" type="text" placeholder="Поиск по тг юзу или ID">
          <button class="admin-search-button" id="adminStatsUsersSearchButton" type="button" aria-label="Найти пользователя">
            <img src="lupa.png" alt="" aria-hidden="true">
          </button>
        </div>
      </div>
      <div class="admin-table-wrap" id="adminStatsUsersTableWrap" hidden>
        <table class="admin-table admin-table--compact">
          <thead><tr><th>Тг юз</th><th>Тг ID</th></tr></thead>
          <tbody id="adminStatsUsersTableBody"><tr><td colspan="2">—</td></tr></tbody>
        </table>
      </div>
      <div class="admin-pagination" id="adminStatsUsersPagination" hidden>
        <button class="admin-action-button" id="adminStatsUsersPrevButton" type="button">← Назад</button>
        <div class="admin-page-indicator" id="adminStatsUsersPageInfo">Страница 1 / 1</div>
        <button class="admin-action-button" id="adminStatsUsersNextButton" type="button">Вперёд →</button>
      </div>
      <section class="admin-user-preview" id="adminStatsUserPreview" hidden>
        <div class="admin-user-preview-head">
          <div class="admin-user-preview-title" id="adminStatsUserPreviewTitle">Профиль пользователя</div>
          <button class="admin-action-button admin-user-preview-close" id="adminStatsUserPreviewClose" type="button">Скрыть</button>
        </div>
        <div class="admin-user-preview-meta" id="adminStatsUserPreviewMeta">Выберите пользователя из таблицы.</div>
        <div class="admin-user-preview-metrics">
          <article class="admin-user-preview-metric">
            <span class="admin-user-preview-metric-label">Покупок</span>
            <span class="admin-user-preview-metric-value" id="adminStatsUserPreviewPurchases">0</span>
          </article>
          <article class="admin-user-preview-metric">
            <span class="admin-user-preview-metric-label">Потрачено</span>
            <span class="admin-user-preview-metric-value" id="adminStatsUserPreviewSpent">0.00 ₽</span>
          </article>
        </div>
        <div class="admin-user-history-title">История покупок</div>
        <div class="admin-user-history-list" id="adminStatsUserPreviewHistory"></div>
        <div class="admin-pagination admin-pagination--inner" id="adminStatsUserPreviewPagination" hidden>
          <button class="admin-action-button" id="adminStatsUserPreviewPrev" type="button">← Назад</button>
          <div class="admin-page-indicator" id="adminStatsUserPreviewPageInfo">Страница 1 / 1</div>
          <button class="admin-action-button" id="adminStatsUserPreviewNext" type="button">Вперёд →</button>
        </div>
      </section>
      <div class="admin-result-card" id="adminStatsResult" hidden></div>
    `;

    const usersButton = document.getElementById("adminStatsUsersButton");
    const salesButton = document.getElementById("adminStatsSalesButton");
    const clearButton = document.getElementById("adminStatsClearButton");
    const usersSearchRow = document.getElementById("adminStatsUsersSearchRow");
    const usersSearchInput = document.getElementById("adminStatsUsersSearchInput");
    const usersSearchButton = document.getElementById("adminStatsUsersSearchButton");
    const usersTableWrap = document.getElementById("adminStatsUsersTableWrap");
    const usersTableBody = document.getElementById("adminStatsUsersTableBody");
    const usersPagination = document.getElementById("adminStatsUsersPagination");
    const usersPrevButton = document.getElementById("adminStatsUsersPrevButton");
    const usersNextButton = document.getElementById("adminStatsUsersNextButton");
    const usersPageInfo = document.getElementById("adminStatsUsersPageInfo");
    const userPreview = document.getElementById("adminStatsUserPreview");
    const userPreviewTitle = document.getElementById("adminStatsUserPreviewTitle");
    const userPreviewClose = document.getElementById("adminStatsUserPreviewClose");
    const userPreviewMeta = document.getElementById("adminStatsUserPreviewMeta");
    const userPreviewPurchases = document.getElementById("adminStatsUserPreviewPurchases");
    const userPreviewSpent = document.getElementById("adminStatsUserPreviewSpent");
    const userPreviewHistory = document.getElementById("adminStatsUserPreviewHistory");
    const userPreviewPagination = document.getElementById("adminStatsUserPreviewPagination");
    const userPreviewPrev = document.getElementById("adminStatsUserPreviewPrev");
    const userPreviewNext = document.getElementById("adminStatsUserPreviewNext");
    const userPreviewPageInfo = document.getElementById("adminStatsUserPreviewPageInfo");
    const resultNode = document.getElementById("adminStatsResult");
    if (
      !usersButton ||
      !salesButton ||
      !clearButton ||
      !usersSearchRow ||
      !usersSearchInput ||
      !usersSearchButton ||
      !usersTableWrap ||
      !usersTableBody ||
      !usersPagination ||
      !usersPrevButton ||
      !usersNextButton ||
      !usersPageInfo ||
      !userPreview ||
      !userPreviewTitle ||
      !userPreviewClose ||
      !userPreviewMeta ||
      !userPreviewPurchases ||
      !userPreviewSpent ||
      !userPreviewHistory ||
      !userPreviewPagination ||
      !userPreviewPrev ||
      !userPreviewNext ||
      !userPreviewPageInfo ||
      !resultNode
    ) {
      return;
    }

    const setButtonsState = (disabled) => {
      usersButton.disabled = disabled;
      salesButton.disabled = disabled;
      clearButton.disabled = disabled;
    };

    let usersPage = 1;
    let usersTotalPages = 1;
    let usersQuery = "";
    let usersLoading = false;
    let usersRequestSeq = 0;
    let activeStatsView = "idle";
    let previewUserId = 0;
    let previewHistoryItems = [];
    let previewHistoryPage = 1;
    const previewHistoryPageSize = 5;
    let previewLoading = false;

    const formatAdminDateTime = (rawValue) => {
      const parsed = new Date(rawValue);
      if (Number.isNaN(parsed.getTime())) {
        return "—";
      }
      const locale = document.documentElement.lang === "en" ? "en-US" : "ru-RU";
      return new Intl.DateTimeFormat(locale, {
        day: "2-digit",
        month: "2-digit",
        year: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      }).format(parsed);
    };

    const getHistoryTypeTitle = (typeValue) => {
      const safeType = String(typeValue || "").trim().toLowerCase();
      if (safeType === "premium") {
        return "Premium";
      }
      if (safeType === "ton") {
        return "TON";
      }
      return "Звёзды";
    };

    const getHistoryStatusMeta = (statusValue) => {
      const safeStatus = String(statusValue || "").trim().toLowerCase();
      if (safeStatus === "success") {
        return { label: "Выполнено", badgeClass: "admin-status-badge--success" };
      }
      if (safeStatus === "pending") {
        return { label: "В работе", badgeClass: "admin-status-badge--limit" };
      }
      if (safeStatus === "error") {
        return { label: "Ошибка", badgeClass: "admin-status-badge--error" };
      }
      return { label: "Проверка", badgeClass: "admin-status-badge--limit" };
    };

    const setUserRowsSelection = () => {
      usersTableBody.querySelectorAll(".admin-user-row").forEach((row) => {
        const rowUserId = Number.parseInt(row.dataset.userId || "0", 10);
        row.classList.toggle("is-selected", previewUserId > 0 && rowUserId === previewUserId);
      });
    };

    const updatePreviewPaginationState = () => {
      const totalPages = Math.max(1, Math.ceil(previewHistoryItems.length / previewHistoryPageSize));
      previewHistoryPage = Math.min(Math.max(1, previewHistoryPage), totalPages);
      userPreviewPagination.hidden = totalPages <= 1;
      userPreviewPrev.disabled = previewLoading || previewHistoryPage <= 1;
      userPreviewNext.disabled = previewLoading || previewHistoryPage >= totalPages;
      userPreviewPageInfo.textContent = `Страница ${previewHistoryPage} / ${totalPages}`;
    };

    const renderPreviewHistoryPage = () => {
      if (previewLoading) {
        userPreviewHistory.innerHTML = '<div class="admin-user-history-empty">Загрузка...</div>';
        updatePreviewPaginationState();
        return;
      }

      if (!previewHistoryItems.length) {
        userPreviewHistory.innerHTML = '<div class="admin-user-history-empty">Покупок пока нет</div>';
        updatePreviewPaginationState();
        return;
      }

      const pageStart = (previewHistoryPage - 1) * previewHistoryPageSize;
      const visibleItems = previewHistoryItems.slice(pageStart, pageStart + previewHistoryPageSize);
      userPreviewHistory.innerHTML = visibleItems
        .map((item) => {
          const statusMeta = getHistoryStatusMeta(item?.status);
          const typeTitle = getHistoryTypeTitle(item?.type);
          const amountText = String(item?.main || "—").trim() || "—";
          const priceText = String(item?.price || "—").trim() || "—";
          const dateText = formatAdminDateTime(item?.createdAt || "");
          return `
            <article class="admin-user-history-item">
              <div class="admin-user-history-main">${escapeHtml(typeTitle)}: <span>${escapeHtml(amountText)}</span></div>
              <span class="admin-status-badge ${escapeHtml(statusMeta.badgeClass)}">${escapeHtml(statusMeta.label)}</span>
              <div class="admin-user-history-side">
                <span class="admin-user-history-price">${escapeHtml(priceText)}</span>
                <span class="admin-user-history-date">${escapeHtml(dateText)}</span>
              </div>
            </article>
          `;
        })
        .join("");
      updatePreviewPaginationState();
    };

    const hideUserPreview = () => {
      previewUserId = 0;
      previewHistoryItems = [];
      previewHistoryPage = 1;
      previewLoading = false;
      userPreview.hidden = true;
      userPreview.style.display = "none";
      userPreviewMeta.textContent = "Выберите пользователя из таблицы.";
      userPreviewPurchases.textContent = "0";
      userPreviewSpent.textContent = "0.00 ₽";
      userPreviewHistory.innerHTML = "";
      updatePreviewPaginationState();
      setUserRowsSelection();
    };

    const setUsersListUiVisible = (visible) => {
      [usersSearchRow, usersTableWrap, usersPagination].forEach((node) => {
        if (!node) {
          return;
        }
        node.hidden = !visible;
        node.style.display = visible ? "" : "none";
      });
    };

    const showUsersListUi = () => {
      setUsersListUiVisible(true);
    };

    const hideUsersListUi = () => {
      setUsersListUiVisible(false);
    };

    const setActiveStatsView = (view) => {
      activeStatsView = view;
      if (view === "users") {
        showUsersListUi();
      } else {
        hideUsersListUi();
        hideUserPreview();
      }
    };

    const updateUsersPaginationState = () => {
      usersPageInfo.textContent = `Страница ${usersPage} / ${usersTotalPages}`;
      usersPrevButton.disabled = usersLoading || usersPage <= 1;
      usersNextButton.disabled = usersLoading || usersPage >= usersTotalPages;
    };

    const setUsersLoadingState = (disabled) => {
      usersLoading = disabled;
      usersSearchInput.disabled = disabled;
      usersSearchButton.disabled = disabled;
      updateUsersPaginationState();
    };

    const setStatsResult = (text = "") => {
      const safeText = String(text || "");
      resultNode.textContent = safeText;
      resultNode.hidden = !safeText;
    };

    const normalizeUsersSearchQuery = (rawValue) => {
      const compact = String(rawValue || "").replace(/\s+/g, "");
      if (!compact) {
        return "";
      }
      const valueWithoutAt = compact.replace(/^@+/, "");
      if (!valueWithoutAt) {
        return "";
      }
      if (/^\d+$/.test(valueWithoutAt)) {
        return valueWithoutAt;
      }
      return `@${valueWithoutAt}`;
    };

    const renderUsersRows = (items) => {
      if (!items.length) {
        usersTableBody.innerHTML = '<tr><td colspan="2">Пользователи не найдены</td></tr>';
        hideUserPreview();
        return;
      }

      usersTableBody.innerHTML = items
        .map((item) => {
          const usernameRaw = String(item?.username || "").trim().replace(/^@+/, "");
          const usernameText = usernameRaw ? `@${usernameRaw}` : "—";
          const userIdValue = Number.parseInt(String(item?.id ?? ""), 10);
          const safeUserId = Number.isFinite(userIdValue) && userIdValue > 0 ? userIdValue : 0;
          const userIdText = safeUserId > 0 ? String(safeUserId) : "—";
          const fullNameText = escapeHtml(String(item?.fullName || "").trim());
          const rowAriaLabel = fullNameText
            ? `Открыть профиль ${escapeHtml(usernameText)} (${fullNameText})`
            : `Открыть профиль ${escapeHtml(usernameText)}`;
          const usernameCell = usernameRaw
            ? `<button class="admin-table-copy" type="button" data-copy-value="@${escapeHtml(usernameRaw)}">@${escapeHtml(usernameRaw)}</button>`
            : "—";
          const userIdCell =
            safeUserId > 0
              ? `<button class="admin-table-copy" type="button" data-copy-value="${escapeHtml(String(safeUserId))}">${escapeHtml(String(safeUserId))}</button>`
              : "—";
          return `<tr class="admin-user-row" data-user-id="${safeUserId}" tabindex="0" role="button" aria-label="${rowAriaLabel}"><td>${usernameCell}</td><td>${userIdCell}</td></tr>`;
        })
        .join("");

      const hasSelectedRow = previewUserId > 0
        ? items.some((item) => Number.parseInt(String(item?.id ?? ""), 10) === previewUserId)
        : false;
      if (previewUserId > 0 && !hasSelectedRow) {
        hideUserPreview();
        return;
      }
      setUserRowsSelection();
    };

    const openUserPreview = async (rawUserId) => {
      if (activeStatsView !== "users") {
        return;
      }
      const userId = Number.parseInt(String(rawUserId || "").trim(), 10);
      if (!Number.isFinite(userId) || userId <= 0) {
        return;
      }

      previewUserId = userId;
      previewHistoryItems = [];
      previewHistoryPage = 1;
      previewLoading = true;
      userPreview.hidden = false;
      userPreview.style.display = "";
      userPreviewTitle.textContent = "Профиль пользователя";
      userPreviewMeta.textContent = `ID: ${userId}`;
      userPreviewPurchases.textContent = "—";
      userPreviewSpent.textContent = "—";
      setUserRowsSelection();
      renderPreviewHistoryPage();
      let previewLoadError = "";

      try {
        const data = await adminRequest("stats_user_profile", {
          userId,
          historyLimit: 40,
        });
        if (activeStatsView !== "users" || previewUserId !== userId) {
          return;
        }

        const user = data.user && typeof data.user === "object" ? data.user : {};
        const usernameRaw = String(user.username || "").trim().replace(/^@+/, "");
        const usernameText = usernameRaw ? `@${usernameRaw}` : "—";
        const fullName = String(user.fullName || "").trim();
        userPreviewTitle.textContent = usernameText;
        userPreviewMeta.textContent = fullName ? `${fullName} | ID: ${userId}` : `ID: ${userId}`;

        const stats = data.stats && typeof data.stats === "object" ? data.stats : {};
        const totalPurchases = Number(stats.totalPurchases);
        const totalSpentRub = Number(stats.totalSpentRub);
        userPreviewPurchases.textContent = formatNumber(Number.isFinite(totalPurchases) ? totalPurchases : 0);
        userPreviewSpent.textContent = `${formatMoney(Number.isFinite(totalSpentRub) ? totalSpentRub : 0)} ₽`;

        previewHistoryItems = Array.isArray(data.history) ? data.history : [];
      } catch (error) {
        if (activeStatsView !== "users" || previewUserId !== userId) {
          return;
        }
        previewLoadError = String(error?.message || "Не удалось загрузить профиль пользователя");
        userPreviewMeta.textContent = `ID: ${userId} | ${previewLoadError}`;
        previewHistoryItems = [];
      } finally {
        if (activeStatsView !== "users" || previewUserId !== userId) {
          return;
        }
        previewLoading = false;
        if (previewLoadError) {
          userPreviewHistory.innerHTML = `<div class="admin-user-history-empty">${escapeHtml(previewLoadError)}</div>`;
          updatePreviewPaginationState();
          return;
        }
        renderPreviewHistoryPage();
      }
    };

    const loadUsersPage = async ({ page, query }) => {
      if (activeStatsView !== "users") {
        return;
      }
      showUsersListUi();
      usersTableBody.innerHTML = '<tr><td colspan="2">Загрузка...</td></tr>';
      const requestSeq = ++usersRequestSeq;
      setUsersLoadingState(true);

      try {
        const data = await adminRequest("stats_users_list", { page, query, limit: 8 });
        if (requestSeq !== usersRequestSeq || activeStatsView !== "users") {
          return;
        }
        const items = Array.isArray(data.items) ? data.items : [];
        const safePage = Number.parseInt(data.page, 10);
        const safeTotalPages = Number.parseInt(data.totalPages, 10);
        const safeTotal = Number.parseInt(data.total, 10);

        usersPage = Number.isFinite(safePage) && safePage > 0 ? safePage : 1;
        usersTotalPages = Number.isFinite(safeTotalPages) && safeTotalPages > 0 ? safeTotalPages : 1;
        usersQuery = normalizeUsersSearchQuery(String(data.query ?? query ?? "").trim());
        usersSearchInput.value = usersQuery;

        renderUsersRows(items);
        setStatsResult(`Пользователей: ${formatNumber(Number.isFinite(safeTotal) ? safeTotal : 0)}`);
      } catch (error) {
        if (requestSeq !== usersRequestSeq || activeStatsView !== "users") {
          return;
        }
        usersTableBody.innerHTML = `<tr><td colspan="2">${escapeHtml(error.message)}</td></tr>`;
        usersPage = 1;
        usersTotalPages = 1;
        setStatsResult(error.message);
      } finally {
        setUsersLoadingState(false);
      }
    };

    usersButton.addEventListener("click", async () => {
      setActiveStatsView("users");
      usersQuery = normalizeUsersSearchQuery(usersSearchInput.value);
      usersSearchInput.value = usersQuery;
      usersPage = 1;
      setStatsResult("");
      await loadUsersPage({ page: usersPage, query: usersQuery });
    });

    salesButton.addEventListener("click", async () => {
      setActiveStatsView("sales");
      usersRequestSeq += 1;
      setStatsResult("");
      setButtonsState(true);
      try {
        const data = await adminRequest("stats_sales");
        const itemsText = (Array.isArray(data.items) ? data.items : [])
          .map((item) => {
            const typeTitle = item.type === "premium" ? "Premium" : "Звёзды";
            const amountValue =
              item.type === "premium"
                ? `${formatNumber(item.amount)} мес.`
                : `${formatNumber(item.amount)} шт.`;
            return `${typeTitle}: ${amountValue} | Выручка ${formatMoney(item.revenue)} ₽ | Себестоимость ${formatMoney(item.costPrice)} ₽`;
          })
          .join("\n");
        const topText = (Array.isArray(data.topBuyers) ? data.topBuyers : [])
          .map((buyer, index) => `${index + 1}. ${buyer.username || "—"} — ${formatMoney(buyer.totalSpent)} ₽`)
          .join("\n");

        setStatsResult([
          itemsText || "Сегодня продаж пока нет.",
          "",
          `Выручка: ${formatMoney(data.totalRevenue)} ₽`,
          `Себестоимость: ${formatMoney(data.totalCostPrice)} ₽`,
          `Прибыль: ${formatMoney(data.profit)} ₽`,
          "",
          "Топ покупателей:",
          topText || "Нет данных",
        ].join("\n"));
      } catch (error) {
        setStatsResult(error.message);
      } finally {
        setButtonsState(false);
      }
    });

    usersSearchButton.addEventListener("click", async () => {
      if (activeStatsView !== "users") {
        return;
      }
      usersQuery = normalizeUsersSearchQuery(usersSearchInput.value);
      usersSearchInput.value = usersQuery;
      usersPage = 1;
      await loadUsersPage({ page: usersPage, query: usersQuery });
    });

    usersSearchInput.addEventListener("input", () => {
      const normalized = normalizeUsersSearchQuery(usersSearchInput.value);
      if (usersSearchInput.value !== normalized) {
        usersSearchInput.value = normalized;
      }
    });

    usersSearchInput.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") {
        return;
      }
      if (activeStatsView !== "users") {
        return;
      }
      event.preventDefault();
      usersSearchButton.click();
    });

    usersTableBody.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) {
        return;
      }
      const copyButton = target.closest("[data-copy-value]");
      if (copyButton) {
        const copyValue = String(copyButton.getAttribute("data-copy-value") || "").trim();
        copyTextToClipboard(copyValue).then((copied) => {
          if (!copied) {
            return;
          }
          copyButton.classList.add("is-copied");
          window.setTimeout(() => copyButton.classList.remove("is-copied"), 900);
        });
        event.preventDefault();
        event.stopPropagation();
        return;
      }
      const row = event.target.closest(".admin-user-row");
      if (!row) {
        return;
      }
      const userId = row.dataset.userId || "";
      openUserPreview(userId);
    });

    usersTableBody.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") {
        return;
      }
      const target = event.target;
      if (target instanceof Element && target.closest("[data-copy-value]")) {
        return;
      }
      const row = event.target.closest(".admin-user-row");
      if (!row) {
        return;
      }
      event.preventDefault();
      const userId = row.dataset.userId || "";
      openUserPreview(userId);
    });

    userPreviewClose.addEventListener("click", () => {
      hideUserPreview();
    });

    userPreviewPrev.addEventListener("click", () => {
      if (previewLoading || previewHistoryPage <= 1) {
        return;
      }
      previewHistoryPage -= 1;
      renderPreviewHistoryPage();
    });

    userPreviewNext.addEventListener("click", () => {
      if (previewLoading) {
        return;
      }
      const totalPages = Math.max(1, Math.ceil(previewHistoryItems.length / previewHistoryPageSize));
      if (previewHistoryPage >= totalPages) {
        return;
      }
      previewHistoryPage += 1;
      renderPreviewHistoryPage();
    });

    usersPrevButton.addEventListener("click", async () => {
      if (activeStatsView !== "users" || usersLoading || usersPage <= 1) {
        return;
      }
      await loadUsersPage({ page: usersPage - 1, query: usersQuery });
    });

    usersNextButton.addEventListener("click", async () => {
      if (activeStatsView !== "users" || usersLoading || usersPage >= usersTotalPages) {
        return;
      }
      await loadUsersPage({ page: usersPage + 1, query: usersQuery });
    });

    clearButton.addEventListener("click", async () => {
      if (!window.confirm("Очистить статистику продаж за последние 24 часа?")) {
        return;
      }
      setActiveStatsView("clear");
      usersRequestSeq += 1;
      setStatsResult("");
      setButtonsState(true);
      try {
        const data = await adminRequest("stats_clear");
        setStatsResult(
          data.deletedCount > 0
            ? `Готово. Удалено записей: ${formatNumber(data.deletedCount)}`
            : "За последние 24 часа продаж не было."
        );
      } catch (error) {
        setStatsResult(error.message);
      } finally {
        setButtonsState(false);
      }
    });

    setActiveStatsView("users");
    usersQuery = normalizeUsersSearchQuery(usersSearchInput.value);
    usersSearchInput.value = usersQuery;
    usersPage = 1;
    updateUsersPaginationState();
    hideUserPreview();
    setStatsResult("");
    await loadUsersPage({ page: usersPage, query: usersQuery });
  };
  const renderPromosSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-form-grid">
        <label class="admin-form-field"><span class="admin-form-label">Код</span><input class="admin-input" id="adminPromoCodeInput" type="text" maxlength="32" placeholder="NEWYEAR2026"></label>
        <label class="admin-form-field"><span class="admin-form-label">Тип эффекта</span><select class="admin-select" id="adminPromoEffectTypeInput"><option value="discount_percent">Скидка %</option><option value="free_stars">Бесплатные звёзды</option></select></label>
        <label class="admin-form-field"><span class="admin-form-label" id="adminPromoEffectValueLabel">Скидка %</span><input class="admin-input" id="adminPromoEffectValueInput" type="number" min="1" step="1" value="10"></label>
        <label class="admin-form-field"><span class="admin-form-label">Мин. звёзд</span><input class="admin-input" id="adminPromoMinStarsInput" type="number" min="0" step="1" value="0"></label>
        <label class="admin-form-field"><span class="admin-form-label">Действует до</span><input class="admin-input admin-input--date" id="adminPromoExpireInput" type="date"></label>
        <label class="admin-form-field"><span class="admin-form-label">Макс. активаций</span><input class="admin-input" id="adminPromoMaxUsesInput" type="number" min="1" step="1" value="100"></label>
        <label class="admin-form-field"><span class="admin-form-label">На пользователя</span><input class="admin-input" id="adminPromoPerUserInput" type="number" min="1" step="1" value="1"></label>
        <label class="admin-form-field"><span class="admin-form-label">Условие</span><select class="admin-select" id="adminPromoConditionInput"><option value="all">Всем</option><option value="buyers">Только покупателям</option></select></label>
        <label class="admin-form-field"><span class="admin-form-label">Применяется к</span><select class="admin-select" id="adminPromoTargetInput"><option value="stars">Звёзды</option><option value="premium">Premium</option><option value="ton">TON</option><option value="all">Всё</option></select></label>
      </div>
      <div class="admin-panel-actions">
        <button class="admin-action-button" id="adminPromoCreateButton" type="button">Создать промокод</button>
        <button class="admin-action-button" id="adminPromoRefreshButton" type="button">Обновить активные</button>
      </div>
      <div class="admin-table-wrap">
        <table class="admin-table">
          <thead><tr><th>Код</th><th>Эффект</th><th>Статус</th><th>Активации</th><th>Истекает</th><th>Тип</th></tr></thead>
          <tbody id="adminPromoTableBody"><tr><td colspan="6">Загрузка...</td></tr></tbody>
        </table>
      </div>
      <div class="admin-split">
        <input class="admin-input" id="adminPromoDeleteInput" type="text" maxlength="32" placeholder="Код для удаления">
        <button class="admin-action-button" id="adminPromoDeleteButton" type="button">Удалить промокод</button>
      </div>
      <div class="admin-result-card" id="adminPromoResult" hidden></div>
    `;

    const codeInput = document.getElementById("adminPromoCodeInput");
    const effectTypeInput = document.getElementById("adminPromoEffectTypeInput");
    const effectValueInput = document.getElementById("adminPromoEffectValueInput");
    const effectValueLabel = document.getElementById("adminPromoEffectValueLabel");
    const minStarsInput = document.getElementById("adminPromoMinStarsInput");
    const expireInput = document.getElementById("adminPromoExpireInput");
    const maxUsesInput = document.getElementById("adminPromoMaxUsesInput");
    const perUserInput = document.getElementById("adminPromoPerUserInput");
    const conditionInput = document.getElementById("adminPromoConditionInput");
    const targetInput = document.getElementById("adminPromoTargetInput");
    const createButton = document.getElementById("adminPromoCreateButton");
    const refreshButton = document.getElementById("adminPromoRefreshButton");
    const deleteInput = document.getElementById("adminPromoDeleteInput");
    const deleteButton = document.getElementById("adminPromoDeleteButton");
    const tableBody = document.getElementById("adminPromoTableBody");
    const resultNode = document.getElementById("adminPromoResult");
    if (
      !codeInput ||
      !effectTypeInput ||
      !effectValueInput ||
      !effectValueLabel ||
      !minStarsInput ||
      !expireInput ||
      !maxUsesInput ||
      !perUserInput ||
      !conditionInput ||
      !targetInput ||
      !createButton ||
      !refreshButton ||
      !deleteInput ||
      !deleteButton ||
      !tableBody ||
      !resultNode
    ) {
      return;
    }

    const setPromoResult = (message = "") => {
      const text = String(message || "").trim();
      if (!text) {
        resultNode.textContent = "";
        resultNode.hidden = true;
        return;
      }
      resultNode.textContent = text;
      resultNode.hidden = false;
    };

    const updateEffectInputs = () => {
      const effectType = effectTypeInput.value;
      if (effectType === "free_stars") {
        effectValueLabel.textContent = "Бесплатные звёзды";
        effectValueInput.min = "1";
        effectValueInput.max = "";
        effectValueInput.placeholder = "Например 100";
      } else {
        effectValueLabel.textContent = "Скидка %";
        effectValueInput.min = "1";
        effectValueInput.max = "100";
        effectValueInput.placeholder = "От 1 до 100";
      }
    };

    expireInput.value = toIsoDate(new Date(Date.now() + 1000 * 60 * 60 * 24 * 30));
    updateEffectInputs();
    effectTypeInput.addEventListener("change", updateEffectInputs);

    const loadPromos = async () => {
      tableBody.innerHTML = '<tr><td colspan="6">Загрузка...</td></tr>';
      try {
        const data = await adminRequest("promo_list");
        const promos = Array.isArray(data.promos) ? data.promos : [];
        if (!promos.length) {
          tableBody.innerHTML = '<tr><td colspan="6">Промокодов нет</td></tr>';
          return;
        }
        tableBody.innerHTML = promos
          .map((promo) => {
            const maxUses = Number(promo.maxUses || 0);
            const usedCount = Number(promo.usesCount || 0);
            const remaining = Number.isFinite(Number(promo.remainingUses))
              ? Math.max(0, Number(promo.remainingUses))
              : Math.max(0, maxUses - usedCount);
            const progress =
              maxUses > 0
                ? Math.max(0, Math.min(100, Math.round((usedCount / maxUses) * 100)))
                : 0;
            const effectType = String(promo.effectType || "discount_percent");
            const effectValue = Number.parseInt(promo.effectValue, 10);
            const rawStatus = String(promo.status || "active").toLowerCase();
            const statusLabel =
              rawStatus === "expired"
                ? "Истёк"
                : rawStatus === "limit"
                  ? "Лимит"
                  : "Активен";
            const effectText =
              effectType === "free_stars"
                ? `Бесплатно ${Number.isFinite(effectValue) ? effectValue : 0}⭐`
                : `${Number.isFinite(effectValue) ? effectValue : Number.parseInt(promo.discount, 10) || 0}%`;
            return `<tr><td><code>${escapeHtml(promo.code || "")}</code></td><td>${escapeHtml(effectText)}</td><td><span class="admin-status-badge admin-status-badge--${escapeHtml(rawStatus)}">${escapeHtml(statusLabel)}</span></td><td><div class="admin-progress"><span class="admin-progress-fill" style="width:${escapeHtml(String(progress))}%"></span></div><div class="admin-progress-meta">${escapeHtml(String(usedCount))}/${escapeHtml(String(maxUses))} (ост: ${escapeHtml(String(remaining))})</div></td><td>${escapeHtml(String(promo.expiresAt || "—"))}</td><td>${escapeHtml(String(promo.target || "stars"))}</td></tr>`;
          })
          .join("");
      } catch (error) {
        tableBody.innerHTML = `<tr><td colspan="6">${escapeHtml(error.message)}</td></tr>`;
      }
    };

    createButton.addEventListener("click", async () => {
      createButton.disabled = true;
      try {
        const payload = {
          code: codeInput.value.trim().toUpperCase(),
          effectType: effectTypeInput.value,
          effectValue: Number.parseInt(effectValueInput.value, 10),
          minStars: Number.parseInt(minStarsInput.value, 10),
          expiresAt: expireInput.value,
          maxUses: Number.parseInt(maxUsesInput.value, 10),
          maxUsesPerUser: Number.parseInt(perUserInput.value, 10),
          condition: conditionInput.value,
          target: targetInput.value,
        };
        await adminRequest("promo_create", payload);
        const maxUsesText = Number.isFinite(payload.maxUses) && payload.maxUses > 0 ? payload.maxUses : "—";
        setPromoResult(`Промокод ${payload.code || "—"} создан. Активации: 0/${maxUsesText}`);
        await loadPromos();
      } catch (error) {
        setPromoResult(error.message);
      } finally {
        createButton.disabled = false;
      }
    });

    refreshButton.addEventListener("click", () => {
      loadPromos().catch(() => {});
    });

    deleteButton.addEventListener("click", async () => {
      const code = deleteInput.value.trim().toUpperCase();
      if (!code) {
        setPromoResult("Введите код для удаления");
        return;
      }
      if (!window.confirm(`Удалить промокод ${code}?`)) {
        return;
      }

      deleteButton.disabled = true;
      try {
        await adminRequest("promo_delete", { code });
        setPromoResult(`Промокод ${code} удалён`);
        deleteInput.value = "";
        await loadPromos();
      } catch (error) {
        setPromoResult(error.message);
      } finally {
        deleteButton.disabled = false;
      }
    });

    setPromoResult("");
    await loadPromos();
  };
  const renderReferralsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-split">
        <input class="admin-input" id="adminRefUserIdInput" type="number" min="1" step="1" placeholder="ID пользователя">
        <button class="admin-action-button" id="adminRefShowButton" type="button">Показать рефералов</button>
      </div>
      <button class="admin-action-button" id="adminRefResetButton" type="button">Сбросить активных</button>
      <div class="admin-result-card" id="adminRefResult">Введите ID пользователя.</div>
    `;

    const userIdInput = document.getElementById("adminRefUserIdInput");
    const showButton = document.getElementById("adminRefShowButton");
    const resetButton = document.getElementById("adminRefResetButton");
    const resultNode = document.getElementById("adminRefResult");
    if (!userIdInput || !showButton || !resetButton || !resultNode) {
      return;
    }

    const getUserId = () => Number.parseInt(userIdInput.value, 10);

    showButton.addEventListener("click", async () => {
      const userId = getUserId();
      if (!Number.isFinite(userId) || userId <= 0) {
        resultNode.textContent = "Введите корректный ID пользователя";
        return;
      }

      showButton.disabled = true;
      resetButton.disabled = true;
      try {
        const data = await adminRequest("referrals_show", { userId });
        const usernames = Array.isArray(data.usernames) ? data.usernames : [];
        resultNode.textContent =
          usernames.length > 0
            ? `Активные рефералы (${usernames.length}):\n${usernames.map((name) => `@${name}`).join("\n")}`
            : `У пользователя ${userId} нет активных рефералов`;
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        showButton.disabled = false;
        resetButton.disabled = false;
      }
    });

    resetButton.addEventListener("click", async () => {
      const userId = getUserId();
      if (!Number.isFinite(userId) || userId <= 0) {
        resultNode.textContent = "Введите корректный ID пользователя";
        return;
      }
      if (!window.confirm(`Сбросить активных рефералов у пользователя ${userId}?`)) {
        return;
      }

      showButton.disabled = true;
      resetButton.disabled = true;
      try {
        const data = await adminRequest("referrals_reset", { userId });
        resultNode.textContent = `Сброшено активных рефералов: ${formatNumber(data.resetCount)}`;
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        showButton.disabled = false;
        resetButton.disabled = false;
      }
    });
  };
  const renderStarsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-result-card" id="adminStarsTotalResult">Загрузка...</div>
      <div class="admin-split admin-split--search">
        <div class="admin-search-wrap">
          <input class="admin-input admin-input--search" id="adminStarsAmountInput" type="number" min="1" step="1" placeholder="Количество звёзд">
          <button class="admin-search-button admin-search-button--update" id="adminStarsRefreshButton" type="button" aria-label="Обновить данные">
            <img src="update.png" alt="" aria-hidden="true">
          </button>
        </div>
      </div>
      <div class="admin-panel-actions">
        <button class="admin-action-button" id="adminStarsAddButton" type="button">Добавить</button>
        <button class="admin-action-button" id="adminStarsRemoveButton" type="button">Убрать</button>
      </div>
      <div class="admin-result-card" id="adminPremiumMonthsTotalResult">Загрузка...</div>
      <div class="admin-split admin-split--search">
        <div class="admin-search-wrap">
          <input class="admin-input admin-input--search" id="adminPremiumMonthsAmountInput" type="number" min="1" step="1" placeholder="Количество месяцев Premium">
          <button class="admin-search-button admin-search-button--update" id="adminPremiumMonthsRefreshButton" type="button" aria-label="Обновить данные">
            <img src="update.png" alt="" aria-hidden="true">
          </button>
        </div>
      </div>
      <div class="admin-panel-actions">
        <button class="admin-action-button" id="adminPremiumMonthsAddButton" type="button">Добавить месяцы</button>
        <button class="admin-action-button" id="adminPremiumMonthsRemoveButton" type="button">Убрать месяцы</button>
      </div>
      <div class="admin-form-grid">
        <label class="admin-form-field"><span class="admin-form-label">50 - 75</span><input class="admin-input" id="adminRate50_75" type="number" min="0.01" step="0.001"></label>
        <label class="admin-form-field"><span class="admin-form-label">76 - 100</span><input class="admin-input" id="adminRate76_100" type="number" min="0.01" step="0.001"></label>
        <label class="admin-form-field"><span class="admin-form-label">101 - 250</span><input class="admin-input" id="adminRate101_250" type="number" min="0.01" step="0.001"></label>
        <label class="admin-form-field"><span class="admin-form-label">251+</span><input class="admin-input" id="adminRate251_plus" type="number" min="0.01" step="0.001"></label>
      </div>
      <button class="admin-action-button" id="adminSaveRatesButton" type="button">Сохранить тарифы</button>
      <div class="admin-result-card" id="adminStarsResult"></div>
    `;

    const totalNode = document.getElementById("adminStarsTotalResult");
    const amountInput = document.getElementById("adminStarsAmountInput");
    const refreshButton = document.getElementById("adminStarsRefreshButton");
    const addButton = document.getElementById("adminStarsAddButton");
    const removeButton = document.getElementById("adminStarsRemoveButton");
    const premiumTotalNode = document.getElementById("adminPremiumMonthsTotalResult");
    const premiumAmountInput = document.getElementById("adminPremiumMonthsAmountInput");
    const premiumRefreshButton = document.getElementById("adminPremiumMonthsRefreshButton");
    const premiumAddButton = document.getElementById("adminPremiumMonthsAddButton");
    const premiumRemoveButton = document.getElementById("adminPremiumMonthsRemoveButton");
    const saveRatesButton = document.getElementById("adminSaveRatesButton");
    const resultNode = document.getElementById("adminStarsResult");
    const rateInputs = {
      "50_75": document.getElementById("adminRate50_75"),
      "76_100": document.getElementById("adminRate76_100"),
      "101_250": document.getElementById("adminRate101_250"),
      "251_plus": document.getElementById("adminRate251_plus"),
    };
    if (
      !totalNode ||
      !amountInput ||
      !refreshButton ||
      !addButton ||
      !removeButton ||
      !premiumTotalNode ||
      !premiumAmountInput ||
      !premiumRefreshButton ||
      !premiumAddButton ||
      !premiumRemoveButton ||
      !saveRatesButton ||
      !resultNode
    ) {
      return;
    }

    const setButtonsState = (disabled) => {
      refreshButton.disabled = disabled;
      addButton.disabled = disabled;
      removeButton.disabled = disabled;
      premiumRefreshButton.disabled = disabled;
      premiumAddButton.disabled = disabled;
      premiumRemoveButton.disabled = disabled;
      saveRatesButton.disabled = disabled;
    };

    const applyRatesToInputs = (rates) => {
      Object.entries(rateInputs).forEach(([key, input]) => {
        if (!(input instanceof HTMLInputElement)) {
          return;
        }
        const numeric = Number(rates?.[key]);
        input.value = Number.isFinite(numeric) && numeric > 0 ? String(Number(numeric.toFixed(3))) : "";
      });
    };

    const collectRatesPayload = () => {
      const payload = {};
      for (const [key, input] of Object.entries(rateInputs)) {
        const numeric = Number(String(input?.value || "").replace(",", ".").trim());
        if (!Number.isFinite(numeric) || numeric <= 0) {
          return null;
        }
        payload[key] = Number(numeric.toFixed(3));
      }
      return payload;
    };

    const loadData = async () => {
      const data = await adminRequest("stars_get");
      totalNode.textContent = `Всего продано звёзд: ${formatNumber(data.totalStars)}`;
      premiumTotalNode.textContent = `Всего продано Premium (мес.): ${formatNumber(data.totalPremiumMonths)}`;
      applyRatesToInputs(data.starRates || {});
    };

    const updateTotal = async ({ metric, mode, inputNode, invalidMessage, addLabel, removeLabel }) => {
      const amount = Number.parseInt(inputNode.value, 10);
      if (!Number.isFinite(amount) || amount <= 0) {
        throw new Error(invalidMessage);
      }
      const data = await adminRequest("stars_update", { metric, mode, amount });
      totalNode.textContent = `Всего продано звёзд: ${formatNumber(data.totalStars)}`;
      premiumTotalNode.textContent = `Всего продано Premium (мес.): ${formatNumber(data.totalPremiumMonths)}`;
      resultNode.textContent =
        mode === "add"
          ? `${addLabel}: ${formatNumber(amount)}`
          : `${removeLabel}: ${formatNumber(amount)}`;
    };

    refreshButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await loadData();
        resultNode.textContent = "Данные обновлены";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    addButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await updateTotal({
          metric: "stars",
          mode: "add",
          inputNode: amountInput,
          invalidMessage: "Введите корректное количество звёзд",
          addLabel: "Добавлено звёзд",
          removeLabel: "Убрано звёзд",
        });
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    removeButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await updateTotal({
          metric: "stars",
          mode: "remove",
          inputNode: amountInput,
          invalidMessage: "Введите корректное количество звёзд",
          addLabel: "Добавлено звёзд",
          removeLabel: "Убрано звёзд",
        });
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    premiumRefreshButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await loadData();
        resultNode.textContent = "Данные Premium обновлены";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    premiumAddButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await updateTotal({
          metric: "premium",
          mode: "add",
          inputNode: premiumAmountInput,
          invalidMessage: "Введите корректное количество месяцев Premium",
          addLabel: "Добавлено Premium (мес.)",
          removeLabel: "Убрано Premium (мес.)",
        });
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    premiumRemoveButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await updateTotal({
          metric: "premium",
          mode: "remove",
          inputNode: premiumAmountInput,
          invalidMessage: "Введите корректное количество месяцев Premium",
          addLabel: "Добавлено Premium (мес.)",
          removeLabel: "Убрано Premium (мес.)",
        });
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    saveRatesButton.addEventListener("click", async () => {
      const ratesPayload = collectRatesPayload();
      if (!ratesPayload) {
        resultNode.textContent = "Введите корректные тарифы больше нуля";
        return;
      }

      setButtonsState(true);
      try {
        const data = await requestAdminRates(ratesPayload);
        applyRatesToInputs(data.starRates || ratesPayload);
        window.starslixMiniappConfig = {
          ...(window.starslixMiniappConfig || {}),
          starRates: { ...(data.starRates || ratesPayload) },
        };
        window.dispatchEvent(new CustomEvent("starslix:pricing-updated"));
        resultNode.textContent = "Тарифы сохранены";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    try {
      await loadData();
      resultNode.textContent = "Данные загружены";
    } catch (error) {
      totalNode.textContent = "Не удалось загрузить данные";
      premiumTotalNode.textContent = "Не удалось загрузить данные";
      resultNode.textContent = error.message;
    }
  };
  const renderPaymentSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-panel-actions">
        <button class="admin-action-button" id="adminPaymentSummaryButton" type="button">Обновить баланс</button>
        <button class="admin-action-button" id="adminPaymentHistoryButton" type="button">История пополнений</button>
      </div>
      <div class="admin-result-card" id="adminPaymentSummaryResult">Загрузка...</div>
      <div class="admin-split">
        <input class="admin-input" id="adminPaymentUserIdInput" type="text" placeholder="ID получателя выплаты">
        <input class="admin-input" id="adminPaymentAmountInput" type="number" min="0.01" step="0.01" placeholder="Сумма выплаты">
      </div>
      <button class="admin-action-button" id="adminPaymentWithdrawButton" type="button">Сделать выплату</button>
      <div class="admin-table-wrap">
        <table class="admin-table">
          <thead><tr><th>Bill ID</th><th>Сумма</th><th>Описание</th><th>Дата</th><th>User ID</th></tr></thead>
          <tbody id="adminPaymentHistoryBody"><tr><td colspan="5">—</td></tr></tbody>
        </table>
      </div>
      <div class="admin-result-card" id="adminPaymentResult"></div>
    `;

    const summaryButton = document.getElementById("adminPaymentSummaryButton");
    const historyButton = document.getElementById("adminPaymentHistoryButton");
    const summaryNode = document.getElementById("adminPaymentSummaryResult");
    const userIdInput = document.getElementById("adminPaymentUserIdInput");
    const amountInput = document.getElementById("adminPaymentAmountInput");
    const withdrawButton = document.getElementById("adminPaymentWithdrawButton");
    const historyBody = document.getElementById("adminPaymentHistoryBody");
    const resultNode = document.getElementById("adminPaymentResult");
    if (
      !summaryButton ||
      !historyButton ||
      !summaryNode ||
      !userIdInput ||
      !amountInput ||
      !withdrawButton ||
      !historyBody ||
      !resultNode
    ) {
      return;
    }

    const setButtonsState = (disabled) => {
      summaryButton.disabled = disabled;
      historyButton.disabled = disabled;
      withdrawButton.disabled = disabled;
    };

    const loadSummary = async () => {
      const data = await adminRequest("payment_summary");
      summaryNode.textContent = [
        `Баланс: ${formatMoney(data.balance)} ₽`,
        `Оплаченных платежей: ${formatNumber(data.paidCount)}`,
        `Сумма оплат: ${formatMoney(data.paidAmount)} ₽`,
      ].join("\n");
    };

    const loadHistory = async () => {
      historyBody.innerHTML = '<tr><td colspan="5">Загрузка...</td></tr>';
      const data = await adminRequest("payment_history", { limit: 10 });
      const items = Array.isArray(data.items) ? data.items : [];
      if (!items.length) {
        historyBody.innerHTML = '<tr><td colspan="5">История пуста</td></tr>';
        return;
      }
      historyBody.innerHTML = items
        .map(
          (item) =>
            `<tr><td>${escapeHtml(item.billId || "—")}</td><td>${formatMoney(item.amount)} ₽</td><td>${escapeHtml(item.description || "—")}</td><td>${escapeHtml(item.date || "—")}</td><td>${escapeHtml(String(item.userId || "—"))}</td></tr>`
        )
        .join("");
    };

    summaryButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await loadSummary();
        resultNode.textContent = "Баланс обновлён";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    historyButton.addEventListener("click", async () => {
      setButtonsState(true);
      try {
        await loadHistory();
        resultNode.textContent = "История обновлена";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    withdrawButton.addEventListener("click", async () => {
      const userId = userIdInput.value.trim();
      const amount = Number(String(amountInput.value || "").replace(",", "."));
      if (!userId) {
        resultNode.textContent = "Введите ID получателя выплаты";
        return;
      }
      if (!Number.isFinite(amount) || amount <= 0) {
        resultNode.textContent = "Введите корректную сумму выплаты";
        return;
      }

      setButtonsState(true);
      try {
        const data = await adminRequest("payment_withdraw", { userId, amount });
        resultNode.textContent = data.success ? "Запрос на выплату отправлен" : "Не удалось выполнить выплату";
        await loadSummary();
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    try {
      await loadSummary();
      resultNode.textContent = "Данные загружены";
    } catch (error) {
      summaryNode.textContent = "Не удалось загрузить баланс";
      resultNode.textContent = error.message;
    }
  };
  const renderBroadcastSection = async () => {
    adminSectionContent.innerHTML = `
      <label class="admin-form-field"><span class="admin-form-label">Текст рассылки</span><textarea class="admin-textarea" id="adminBroadcastTextInput" placeholder="Введите текст рассылки"></textarea></label>
      <div class="admin-upload">
        <label class="admin-upload-label" for="adminBroadcastImageInput">Выбрать фото</label>
        <input id="adminBroadcastImageInput" type="file" accept="image/*" hidden>
        <span class="admin-upload-name" id="adminBroadcastImageName">Фото не выбрано</span>
      </div>
      <label class="admin-form-field"><span class="admin-form-label">Кнопки (каждая строка: Текст - https://ссылка)</span><textarea class="admin-textarea" id="adminBroadcastButtonsInput" placeholder="Наш сайт - https://example.com"></textarea></label>
      <button class="admin-action-button" id="adminBroadcastSendButton" type="button">Отправить рассылку</button>
      <div class="admin-result-card" id="adminBroadcastResult">Можно отправить текст, фото или текст с фото.</div>
    `;

    const textInput = document.getElementById("adminBroadcastTextInput");
    const imageInput = document.getElementById("adminBroadcastImageInput");
    const imageNameNode = document.getElementById("adminBroadcastImageName");
    const buttonsInput = document.getElementById("adminBroadcastButtonsInput");
    const sendButton = document.getElementById("adminBroadcastSendButton");
    const resultNode = document.getElementById("adminBroadcastResult");
    if (!textInput || !imageInput || !imageNameNode || !buttonsInput || !sendButton || !resultNode) {
      return;
    }

    imageInput.addEventListener("change", () => {
      const file = imageInput.files?.[0];
      imageNameNode.textContent = file ? file.name : "Фото не выбрано";
    });

    sendButton.addEventListener("click", async () => {
      const text = textInput.value.trim();
      const file = imageInput.files?.[0] || null;
      if (!text && !file) {
        resultNode.textContent = "Добавьте текст и/или фото для рассылки";
        return;
      }

      let buttons = [];
      try {
        buttons = parseButtonsText(buttonsInput.value);
      } catch (error) {
        resultNode.textContent = error.message;
        return;
      }

      sendButton.disabled = true;
      try {
        let imageBase64 = "";
        let imageMime = "";
        let imageName = "";
        if (file) {
          const dataUrl = await fileToDataUrl(file);
          imageBase64 = stripDataUrlPrefix(dataUrl);
          imageMime = String(file.type || "").trim();
          imageName = String(file.name || "").trim();
        }

        const data = await adminRequest("broadcast_send", {
          text,
          buttons,
          imageBase64,
          imageMime,
          imageName,
        });

        resultNode.textContent = `Рассылка завершена\nУспешно: ${formatNumber(data.success)}\nОшибок: ${formatNumber(data.fail)}`;
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        sendButton.disabled = false;
      }
    });
  };
  const renderFindUserSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-split admin-split--search">
        <div class="admin-search-wrap">
          <input class="admin-input admin-input--search" id="adminFindUserQueryInput" type="text" placeholder="Username или ID">
          <button class="admin-search-button" id="adminFindUserButton" type="button" aria-label="Найти пользователя">
            <img src="lupa.png" alt="" aria-hidden="true">
          </button>
        </div>
      </div>
      <div class="admin-result-card" id="adminFindUserResult">Введите username (например @example) или ID.</div>
    `;

    const queryInput = document.getElementById("adminFindUserQueryInput");
    const findButton = document.getElementById("adminFindUserButton");
    const resultNode = document.getElementById("adminFindUserResult");
    if (!queryInput || !findButton || !resultNode) {
      return;
    }

    const normalizeFindQuery = (rawValue) => {
      const compact = String(rawValue || "").replace(/\s+/g, "");
      if (!compact) {
        return "";
      }
      const withoutAt = compact.replace(/^@+/, "");
      if (!withoutAt) {
        return "";
      }
      if (/^\d+$/.test(withoutAt)) {
        return withoutAt;
      }
      return `@${withoutAt}`;
    };

    findButton.addEventListener("click", async () => {
      const query = normalizeFindQuery(queryInput.value);
      queryInput.value = query;
      if (!query) {
        resultNode.textContent = "Введите username или ID";
        return;
      }

      findButton.disabled = true;
      try {
        const data = await adminRequest("user_find", { query });
        const user = data.user || {};
        const purchases = Array.isArray(data.purchases) ? data.purchases : [];
        const promos = Array.isArray(data.usedPromos) ? data.usedPromos : [];
        const payments = Array.isArray(data.payments) ? data.payments : [];

        const purchaseText = purchases.length
          ? purchases.map((item) => `${item.itemType}: ${item.amount} за ${formatMoney(item.cost)} ₽ (${item.createdAt || "—"})`).join("\n")
          : "Нет покупок";

        const paymentText = payments.length
          ? payments.map((item) => `${formatMoney(item.amount)} ₽ - ${item.status || "—"} (${item.date || "—"})`).join("\n")
          : "Нет платежей";

        resultNode.textContent = [
          `ID: ${user.id || "—"}`,
          `Username: ${user.username || "—"}`,
          `Имя: ${[user.firstName || "", user.lastName || ""].join(" ").trim() || "—"}`,
          `Реф. код: ${user.refCode || "—"}`,
          `Рефералов: ${formatNumber(user.referralsCount || 0)} (с покупкой: ${formatNumber(user.referralsWithPurchase || 0)})`,
          "",
          "Покупки:",
          purchaseText,
          "",
          `Промокоды: ${promos.length ? promos.join(", ") : "Нет"}`,
          "",
          "Платежи:",
          paymentText,
        ].join("\n");
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        findButton.disabled = false;
      }
    });

    queryInput.addEventListener("input", () => {
      const normalized = normalizeFindQuery(queryInput.value);
      if (queryInput.value !== normalized) {
        queryInput.value = normalized;
      }
    });

    queryInput.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") {
        return;
      }
      event.preventDefault();
      findButton.click();
    });
  };

  const renderAddStarsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-form-grid">
        <label class="admin-form-field"><span class="admin-form-label">ID пользователя</span><input class="admin-input" id="adminGrantUserIdInput" type="number" min="1" step="1" placeholder="Например 123456789"></label>
        <label class="admin-form-field"><span class="admin-form-label">Количество звёзд</span><input class="admin-input" id="adminGrantAmountInput" type="number" min="1" step="1" placeholder="Например 100"></label>
      </div>
      <button class="admin-action-button" id="adminGrantSubmitButton" type="button">Начислить звёзды</button>
      <div class="admin-result-card" id="adminGrantResult"></div>
    `;

    const userIdInput = document.getElementById("adminGrantUserIdInput");
    const amountInput = document.getElementById("adminGrantAmountInput");
    const submitButton = document.getElementById("adminGrantSubmitButton");
    const resultNode = document.getElementById("adminGrantResult");
    if (!userIdInput || !amountInput || !submitButton || !resultNode) {
      return;
    }

    submitButton.addEventListener("click", async () => {
      const userId = Number.parseInt(userIdInput.value, 10);
      const amount = Number.parseInt(amountInput.value, 10);
      if (!Number.isFinite(userId) || userId <= 0) {
        resultNode.textContent = "Введите корректный ID пользователя";
        return;
      }
      if (!Number.isFinite(amount) || amount <= 0) {
        resultNode.textContent = "Введите корректное количество звёзд";
        return;
      }

      submitButton.disabled = true;
      try {
        const data = await adminRequest("add_stars", { userId, amount });
        resultNode.textContent = `Начислено ${formatNumber(data.amount)} звёзд пользователю ${data.userId}. Уведомление: ${data.notificationSent ? "отправлено" : "не отправлено"}.`;
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        submitButton.disabled = false;
      }
    });
  };

  const renderAdminsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-split admin-split--search">
        <div class="admin-search-wrap">
          <input class="admin-input admin-input--search" id="adminManageUserIdInput" type="number" min="1" step="1" placeholder="ID пользователя">
          <button class="admin-search-button admin-search-button--update" id="adminManageRefreshButton" type="button" aria-label="Обновить список">
            <img src="update.png" alt="" aria-hidden="true">
          </button>
        </div>
      </div>
      <div class="admin-panel-actions">
        <button class="admin-action-button" id="adminManageAddButton" type="button">Добавить админа</button>
        <button class="admin-action-button" id="adminManageRemoveButton" type="button">Убрать админа</button>
      </div>
      <div class="admin-table-wrap">
        <table class="admin-table admin-table--compact">
          <thead><tr><th>Тг ID</th><th>Тг юз</th><th>Роль</th></tr></thead>
          <tbody id="adminManageTableBody"><tr><td colspan="3">Загрузка...</td></tr></tbody>
        </table>
      </div>
      <div class="admin-result-card" id="adminManageResult">Только владелец может добавлять и удалять админов.</div>
    `;

    const userIdInput = document.getElementById("adminManageUserIdInput");
    const refreshButton = document.getElementById("adminManageRefreshButton");
    const addButton = document.getElementById("adminManageAddButton");
    const removeButton = document.getElementById("adminManageRemoveButton");
    const tableBody = document.getElementById("adminManageTableBody");
    const resultNode = document.getElementById("adminManageResult");
    if (!userIdInput || !refreshButton || !addButton || !removeButton || !tableBody || !resultNode) {
      return;
    }

    let canManage = false;

    const setButtonsState = (disabled) => {
      refreshButton.disabled = disabled;
      addButton.disabled = disabled || !canManage;
      removeButton.disabled = disabled || !canManage;
      userIdInput.disabled = disabled || !canManage;
    };

    const renderTable = (items) => {
      if (!items.length) {
        tableBody.innerHTML = '<tr><td colspan="3">Админы не найдены</td></tr>';
        return;
      }
      tableBody.innerHTML = items
        .map((item) => {
          const idText = escapeHtml(String(item?.id ?? "—"));
          const usernameRaw = String(item?.username || "").trim().replace(/^@+/, "");
          const usernameText = usernameRaw ? `@${escapeHtml(usernameRaw)}` : "—";
          const roleText = item?.isOwner ? "Владелец" : "Админ";
          return `<tr><td>${idText}</td><td>${usernameText}</td><td>${roleText}</td></tr>`;
        })
        .join("");
    };

    const loadAdmins = async () => {
      setButtonsState(true);
      tableBody.innerHTML = '<tr><td colspan="3">Загрузка...</td></tr>';
      try {
        const data = await adminRequest("admins_list");
        canManage = Boolean(data.canManage);
        renderTable(Array.isArray(data.items) ? data.items : []);
        resultNode.textContent = canManage
          ? "Вы можете добавлять и удалять админов."
          : "Только владелец может добавлять и удалять админов.";
      } catch (error) {
        tableBody.innerHTML = `<tr><td colspan="3">${escapeHtml(error.message)}</td></tr>`;
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    };

    addButton.addEventListener("click", async () => {
      const userId = Number.parseInt(userIdInput.value, 10);
      if (!Number.isFinite(userId) || userId <= 0) {
        resultNode.textContent = "Введите корректный ID пользователя";
        return;
      }
      setButtonsState(true);
      try {
        const data = await adminRequest("admin_add", { userId });
        renderTable(Array.isArray(data.items) ? data.items : []);
        resultNode.textContent = data.changed
          ? `Пользователь ${userId} добавлен в админы`
          : `Пользователь ${userId} уже админ`;
        userIdInput.value = "";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    removeButton.addEventListener("click", async () => {
      const userId = Number.parseInt(userIdInput.value, 10);
      if (!Number.isFinite(userId) || userId <= 0) {
        resultNode.textContent = "Введите корректный ID пользователя";
        return;
      }
      if (!window.confirm(`Убрать пользователя ${userId} из админов?`)) {
        return;
      }

      setButtonsState(true);
      try {
        const data = await adminRequest("admin_remove", { userId });
        renderTable(Array.isArray(data.items) ? data.items : []);
        resultNode.textContent = data.changed
          ? `Пользователь ${userId} удалён из админов`
          : "Изменений нет";
        userIdInput.value = "";
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setButtonsState(false);
      }
    });

    refreshButton.addEventListener("click", () => {
      loadAdmins().catch(() => {});
    });

    await loadAdmins();
  };

  const renderOperationsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-split admin-split--search">
        <div class="admin-search-wrap">
          <input class="admin-input admin-input--search" id="adminOperationsSearchInput" type="text" placeholder="Поиск по ID операции, @юзу, ID, промокоду, дате (например 11:02)">
          <button class="admin-search-button" id="adminOperationsSearchButton" type="button" aria-label="Поиск операций">
            <img src="lupa.png" alt="" aria-hidden="true">
          </button>
        </div>
      </div>
      <div class="admin-table-wrap" id="adminOperationsTableWrap" hidden>
        <table class="admin-table admin-table--operations">
          <thead><tr><th>ID операции</th><th>Дата</th><th>Покупатель</th><th>Аккаунт</th><th>Товар</th><th>Сумма</th><th>Статус</th><th>Промокод</th><th>Канал</th></tr></thead>
          <tbody id="adminOperationsTableBody"><tr><td colspan="9">Загрузка...</td></tr></tbody>
        </table>
      </div>
      <div class="admin-pagination" id="adminOperationsPagination" hidden>
        <button class="admin-action-button" id="adminOperationsPrevButton" type="button">← Назад</button>
        <div class="admin-page-indicator" id="adminOperationsPageInfo">Страница 1 / 1</div>
        <button class="admin-action-button" id="adminOperationsNextButton" type="button">Вперёд →</button>
      </div>
      <div class="admin-result-card" id="adminOperationsResult">Введите запрос и нажмите поиск.</div>
    `;

    const searchInput = document.getElementById("adminOperationsSearchInput");
    const searchButton = document.getElementById("adminOperationsSearchButton");
    const tableWrap = document.getElementById("adminOperationsTableWrap");
    const paginationWrap = document.getElementById("adminOperationsPagination");
    const tableBody = document.getElementById("adminOperationsTableBody");
    const prevButton = document.getElementById("adminOperationsPrevButton");
    const nextButton = document.getElementById("adminOperationsNextButton");
    const pageInfo = document.getElementById("adminOperationsPageInfo");
    const resultNode = document.getElementById("adminOperationsResult");
    if (
      !searchInput ||
      !searchButton ||
      !tableWrap ||
      !paginationWrap ||
      !tableBody ||
      !prevButton ||
      !nextButton ||
      !pageInfo ||
      !resultNode
    ) {
      return;
    }

    let page = 1;
    let totalPages = 1;
    let total = 0;
    let query = "";
    let loading = false;

    const formatOperationDate = (rawValue) => {
      const parsed = new Date(String(rawValue || ""));
      if (Number.isNaN(parsed.getTime())) {
        return "—";
      }
      return new Intl.DateTimeFormat("ru-RU", {
        day: "2-digit",
        month: "2-digit",
        year: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }).format(parsed);
    };

    const getOperationTypeTitle = (value) => {
      const type = String(value || "").trim().toLowerCase();
      if (type === "premium") {
        return "Premium";
      }
      if (type === "ton") {
        return "TON";
      }
      return "Звёзды";
    };

    const getOperationStatusMeta = (value) => {
      const status = String(value || "").trim().toLowerCase();
      if (status === "error") {
        return { label: "Ошибка", className: "admin-status-badge--error" };
      }
      if (status === "pending" || status === "warning") {
        return { label: "В работе", className: "admin-status-badge--pending" };
      }
      return { label: "Выполнено", className: "admin-status-badge--success" };
    };

    const getOperationChannelTitle = (value) => {
      const channel = String(value || "").trim().toLowerCase();
      if (channel === "legacy") {
        return "Старый бот";
      }
      if (channel === "miniapp_test" || channel === "miniapp") {
        return "Miniapp тест";
      }
      return channel || "—";
    };

    const updatePagination = () => {
      pageInfo.textContent = `Страница ${page} / ${totalPages}`;
      prevButton.disabled = loading || page <= 1;
      nextButton.disabled = loading || page >= totalPages;
      searchButton.disabled = loading;
      searchInput.disabled = loading;
    };

    const setLoadingState = (disabled) => {
      loading = disabled;
      updatePagination();
    };

    const renderRows = (items) => {
      if (!items.length) {
        tableBody.innerHTML = '<tr><td colspan="9">Операций пока нет</td></tr>';
        return;
      }

      tableBody.innerHTML = items
        .map((item) => {
          const operationIdRaw = String(item?.operationId || "—").trim();
          const operationId = escapeHtml(operationIdRaw || "—");
          const dateText = escapeHtml(formatOperationDate(item?.createdAt));

          const buyerUserId = Number.parseInt(String(item?.buyerUserId || ""), 10);
          const buyerUsernameRaw = String(item?.buyerUsername || "").trim().replace(/^@+/, "");
          const buyerFullName = String(item?.buyerFullName || "").trim();
          const buyerParts = [];
          if (buyerUsernameRaw) {
            buyerParts.push(`@${escapeHtml(buyerUsernameRaw)}`);
          }
          if (buyerFullName) {
            buyerParts.push(escapeHtml(buyerFullName));
          }
          if (Number.isFinite(buyerUserId) && buyerUserId > 0) {
            buyerParts.push(`ID ${escapeHtml(String(buyerUserId))}`);
          }
          const buyerText = buyerParts.length ? buyerParts.join("<br>") : "—";

          const targetRaw = String(item?.targetUsername || "").trim().replace(/^@+/, "");
          const targetText = targetRaw ? `@${escapeHtml(targetRaw)}` : "—";

          const typeText = getOperationTypeTitle(item?.itemType);
          const amountText = String(item?.main || item?.amountRaw || "—").trim() || "—";
          const itemText = `${escapeHtml(typeText)}: ${escapeHtml(amountText)}`;

          const priceText = escapeHtml(String(item?.price || "—"));
          const statusMeta = getOperationStatusMeta(item?.status);
          const statusBadge = `<span class="admin-status-badge ${statusMeta.className}">${statusMeta.label}</span>`;

          const promoCode = String(item?.promoCode || "").trim().toUpperCase();
          const promoDiscountValue = Number(item?.promoDiscount);
          const promoSuffix =
            promoCode && Number.isFinite(promoDiscountValue) && promoDiscountValue > 0
              ? ` (${Math.round(promoDiscountValue)}%)`
              : "";
          const promoText = promoCode ? `${escapeHtml(promoCode)}${promoSuffix}` : "—";
          const channelText = escapeHtml(getOperationChannelTitle(item?.channel || item?.operationSource));
          const operationCell =
            operationIdRaw && operationIdRaw !== "—"
              ? `<button class="admin-table-copy admin-table-copy--code" type="button" data-copy-value="${operationId}">${operationId}</button>`
              : `<code>${operationId}</code>`;
          return `<tr><td>${operationCell}</td><td>${dateText}</td><td>${buyerText}</td><td>${targetText}</td><td>${itemText}</td><td>${priceText}</td><td>${statusBadge}</td><td>${promoText}</td><td>${channelText}</td></tr>`;
        })
        .join("");
    };

    const loadOperationsPage = async (nextPage, nextQuery = query) => {
      const safeQuery = String(nextQuery || "").trim();
      if (!safeQuery) {
        query = "";
        page = 1;
        totalPages = 1;
        total = 0;
        searchInput.value = "";
        tableWrap.hidden = true;
        paginationWrap.hidden = true;
        tableBody.innerHTML = '<tr><td colspan="9">Введите запрос и нажмите поиск</td></tr>';
        resultNode.textContent = "Введите ID операции или данные пользователя.";
        updatePagination();
        return;
      }

      setLoadingState(true);
      tableWrap.hidden = false;
      paginationWrap.hidden = false;
      tableBody.innerHTML = '<tr><td colspan="9">Загрузка...</td></tr>';
      try {
        const data = await adminRequest("operations_list", {
          page: nextPage,
          limit: 8,
          query: safeQuery,
        });
        const items = Array.isArray(data.items) ? data.items : [];
        const safePage = Number.parseInt(data.page, 10);
        const safeTotalPages = Number.parseInt(data.totalPages, 10);
        const safeTotal = Number.parseInt(data.total, 10);

        page = Number.isFinite(safePage) && safePage > 0 ? safePage : 1;
        totalPages = Number.isFinite(safeTotalPages) && safeTotalPages > 0 ? safeTotalPages : 1;
        total = Number.isFinite(safeTotal) && safeTotal >= 0 ? safeTotal : 0;
        query = String(data.query ?? safeQuery ?? "").trim();
        searchInput.value = query;

        renderRows(items);
        resultNode.textContent = `Операций: ${formatNumber(total)}`;
        paginationWrap.hidden = totalPages <= 1;
      } catch (error) {
        tableBody.innerHTML = `<tr><td colspan="9">${escapeHtml(error.message)}</td></tr>`;
        page = 1;
        totalPages = 1;
        total = 0;
        resultNode.textContent = error.message;
        paginationWrap.hidden = true;
      } finally {
        setLoadingState(false);
      }
    };

    searchButton.addEventListener("click", () => {
      const nextQuery = String(searchInput.value || "").trim();
      loadOperationsPage(1, nextQuery).catch(() => {});
    });

    tableBody.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) {
        return;
      }
      const copyNode = target.closest("[data-copy-value]");
      if (!copyNode) {
        return;
      }
      const copyValue = String(copyNode.getAttribute("data-copy-value") || "").trim();
      copyTextToClipboard(copyValue).then((copied) => {
        if (!copied) {
          return;
        }
        copyNode.classList.add("is-copied");
        window.setTimeout(() => copyNode.classList.remove("is-copied"), 900);
      });
      event.preventDefault();
    });

    searchInput.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") {
        return;
      }
      event.preventDefault();
      const nextQuery = String(searchInput.value || "").trim();
      loadOperationsPage(1, nextQuery).catch(() => {});
    });

    prevButton.addEventListener("click", () => {
      if (loading || page <= 1) {
        return;
      }
      loadOperationsPage(page - 1).catch(() => {});
    });

    nextButton.addEventListener("click", () => {
      if (loading || page >= totalPages) {
        return;
      }
      loadOperationsPage(page + 1).catch(() => {});
    });

    updatePagination();
  };

  const renderSupportSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-split admin-split--search">
        <div class="admin-search-wrap">
          <input class="admin-input admin-input--search" id="adminSupportSearchInput" type="text" placeholder="Поиск по ID, username или названию чата">
          <button class="admin-search-button" id="adminSupportSearchButton" type="button" aria-label="Найти чат">
            <img src="lupa.png" alt="" aria-hidden="true">
          </button>
        </div>
      </div>
      <div class="admin-support-layout" id="adminSupportLayout">
        <section class="admin-support-chats" id="adminSupportChatsPanel">
          <div class="admin-support-chats-list" id="adminSupportChatsList">
            <div class="admin-support-empty">Загрузка...</div>
          </div>
          <div class="admin-pagination" id="adminSupportPagination">
            <button class="admin-action-button" id="adminSupportPrevButton" type="button">← Назад</button>
            <div class="admin-page-indicator" id="adminSupportPageInfo">Страница 1 / 1</div>
            <button class="admin-action-button" id="adminSupportNextButton" type="button">Вперёд →</button>
          </div>
        </section>
        <section class="admin-support-pane" id="adminSupportPane" hidden>
          <button class="admin-action-button admin-support-back" id="adminSupportBackButton" type="button">← К чатам</button>
          <div class="admin-support-head">
            <input class="admin-input" id="adminSupportTitleInput" type="text" placeholder="Название чата">
            <button class="admin-action-button" id="adminSupportRenameButton" type="button">Переименовать</button>
            <button class="admin-action-button" id="adminSupportDeleteButton" type="button">Удалить</button>
          </div>
          <div class="admin-user-preview-meta" id="adminSupportChatMeta">Выберите чат из списка.</div>
          <div class="admin-support-messages" id="adminSupportMessages">
            <div class="admin-support-empty">Выберите чат из списка.</div>
          </div>
          <div class="admin-support-compose">
            <div class="admin-support-compose-row">
              <label class="admin-support-upload admin-support-upload--icon" for="adminSupportReplyPhotoInput" aria-label="Загрузить фото">
                <span aria-hidden="true">&#128206;</span>
              </label>
              <textarea class="admin-textarea admin-support-compose-input" id="adminSupportReplyInput" placeholder="Напишите сообщение"></textarea>
              <button class="admin-action-button" id="adminSupportSendButton" type="button">Отправить</button>
            </div>
            <div class="admin-support-upload-meta">
              <span class="admin-support-upload-name" id="adminSupportReplyPhotoName">Без файла</span>
              <button class="admin-support-photo-clear" id="adminSupportReplyPhotoClearButton" type="button" aria-label="Убрать фото" hidden>✕</button>
            </div>
            <input id="adminSupportReplyPhotoInput" type="file" accept="image/*" hidden>
          </div>
        </section>
      </div>
      <div class="admin-result-card" id="adminSupportResult"></div>
    `;

    const searchInput = document.getElementById("adminSupportSearchInput");
    const searchButton = document.getElementById("adminSupportSearchButton");
    const layout = document.getElementById("adminSupportLayout");
    const chatsPanel = document.getElementById("adminSupportChatsPanel");
    const chatsList = document.getElementById("adminSupportChatsList");
    const pagination = document.getElementById("adminSupportPagination");
    const prevButton = document.getElementById("adminSupportPrevButton");
    const nextButton = document.getElementById("adminSupportNextButton");
    const pageInfo = document.getElementById("adminSupportPageInfo");
    const pane = document.getElementById("adminSupportPane");
    const backButton = document.getElementById("adminSupportBackButton");
    const titleInput = document.getElementById("adminSupportTitleInput");
    const renameButton = document.getElementById("adminSupportRenameButton");
    const deleteButton = document.getElementById("adminSupportDeleteButton");
    const chatMeta = document.getElementById("adminSupportChatMeta");
    const messagesNode = document.getElementById("adminSupportMessages");
    const replyInput = document.getElementById("adminSupportReplyInput");
    const replyPhotoInput = document.getElementById("adminSupportReplyPhotoInput");
    const replyPhotoName = document.getElementById("adminSupportReplyPhotoName");
    const replyPhotoClearButton = document.getElementById("adminSupportReplyPhotoClearButton");
    const sendButton = document.getElementById("adminSupportSendButton");
    const resultNode = document.getElementById("adminSupportResult");
    if (
      !searchInput ||
      !searchButton ||
      !layout ||
      !chatsPanel ||
      !chatsList ||
      !pagination ||
      !prevButton ||
      !nextButton ||
      !pageInfo ||
      !pane ||
      !backButton ||
      !titleInput ||
      !renameButton ||
      !deleteButton ||
      !chatMeta ||
      !messagesNode ||
      !replyInput ||
      !replyPhotoInput ||
      !replyPhotoName ||
      !replyPhotoClearButton ||
      !sendButton ||
      !resultNode
    ) {
      return;
    }

    const formatSupportDate = (rawValue) => {
      const parsed = new Date(String(rawValue || ""));
      if (Number.isNaN(parsed.getTime())) {
        return "—";
      }
      return new Intl.DateTimeFormat("ru-RU", {
        day: "2-digit",
        month: "2-digit",
        year: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      }).format(parsed);
    };

    const sanitizeChatTitle = (value) =>
      String(value || "")
        .replace(/(?:^|[•·|])\s*0+\s*уведом[^\n•·|]*/gi, " ")
        .replace(/(?:^|[•·|])\s*0+\s*notifications?[^\n•·|]*/gi, " ")
        .replace(/\s{2,}/g, " ")
        .replace(/^[•·|\s-]+/, "")
        .replace(/[•·|\s-]+$/, "")
        .trim();

    const resolvePhotoUrl = (value) => {
      const raw = String(value || "").trim();
      if (!raw) {
        return "";
      }
      if (/^https?:\/\//i.test(raw)) {
        return raw;
      }
      const normalized = raw.startsWith("/") ? raw : `/${raw}`;
      return buildApiUrl(normalized);
    };

    const compressImageForUpload = (file, { maxSide = 1600, quality = 0.82 } = {}) =>
      new Promise((resolve) => {
        if (!(file instanceof File) || !String(file.type || "").toLowerCase().startsWith("image/")) {
          resolve(file);
          return;
        }
        const objectUrl = URL.createObjectURL(file);
        const image = new Image();
        image.onload = () => {
          try {
            const width = image.naturalWidth || image.width || 0;
            const height = image.naturalHeight || image.height || 0;
            if (!width || !height) {
              URL.revokeObjectURL(objectUrl);
              resolve(file);
              return;
            }
            const scale = Math.min(1, maxSide / Math.max(width, height));
            const targetWidth = Math.max(1, Math.round(width * scale));
            const targetHeight = Math.max(1, Math.round(height * scale));
            const canvas = document.createElement("canvas");
            canvas.width = targetWidth;
            canvas.height = targetHeight;
            const ctx = canvas.getContext("2d");
            if (!ctx) {
              URL.revokeObjectURL(objectUrl);
              resolve(file);
              return;
            }
            ctx.drawImage(image, 0, 0, targetWidth, targetHeight);
            canvas.toBlob(
              (blob) => {
                URL.revokeObjectURL(objectUrl);
                if (blob) {
                  resolve(new File([blob], file.name || "image.jpg", { type: "image/jpeg" }));
                  return;
                }
                resolve(file);
              },
              "image/jpeg",
              quality
            );
          } catch {
            URL.revokeObjectURL(objectUrl);
            resolve(file);
          }
        };
        image.onerror = () => {
          URL.revokeObjectURL(objectUrl);
          resolve(file);
        };
        image.src = objectUrl;
      });

    const ensurePhotoViewer = () => {
      let root = document.getElementById("supportPhotoViewer");
      if (root) {
        return root;
      }
      root = document.createElement("div");
      root.id = "supportPhotoViewer";
      root.className = "support-photo-viewer";
      root.hidden = true;
      root.innerHTML = `
        <div class="support-photo-viewer-backdrop" data-close-photo-viewer></div>
        <img class="support-photo-viewer-image" id="supportPhotoViewerImage" alt="Фото">
        <button class="support-photo-viewer-close" type="button" data-close-photo-viewer aria-label="Закрыть">✕</button>
      `;
      document.body.appendChild(root);
      root.addEventListener("click", (event) => {
        const target = event.target;
        if (!(target instanceof Element)) {
          return;
        }
        if (target.closest("[data-close-photo-viewer]")) {
          root.hidden = true;
        }
      });
      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && !root.hidden) {
          root.hidden = true;
        }
      });
      return root;
    };

    const openSupportPhoto = (url) => {
      const safeUrl = String(url || "").trim();
      if (!safeUrl) {
        return;
      }
      const viewer = ensurePhotoViewer();
      const imageNode = viewer.querySelector("#supportPhotoViewerImage");
      if (!(imageNode instanceof HTMLImageElement)) {
        return;
      }
      imageNode.src = safeUrl;
      viewer.hidden = false;
    };

    let page = 1;
    let totalPages = 1;
    let total = 0;
    let query = "";
    let loadingList = false;
    let loadingChat = false;
    let selectedChatId = 0;
    let replyPhotoFile = null;

    const setSupportPaneMode = (isChatOpen) => {
      const active = Boolean(isChatOpen);
      layout.classList.toggle("is-chat-open", active);
      chatsPanel.hidden = active;
      pane.hidden = !active;
    };

    const updatePagination = () => {
      pageInfo.textContent = `Страница ${page} / ${totalPages}`;
      prevButton.disabled = loadingList || page <= 1;
      nextButton.disabled = loadingList || page >= totalPages;
      searchButton.disabled = loadingList;
      searchInput.disabled = loadingList;
      pagination.hidden = totalPages <= 1;
    };

    const setListLoadingState = (disabled) => {
      loadingList = disabled;
      updatePagination();
    };

    const setChatLoadingState = (disabled) => {
      loadingChat = disabled;
      titleInput.disabled = disabled || selectedChatId <= 0;
      renameButton.disabled = disabled || selectedChatId <= 0;
      deleteButton.disabled = disabled || selectedChatId <= 0;
      replyInput.disabled = disabled || selectedChatId <= 0;
      replyPhotoInput.disabled = disabled || selectedChatId <= 0;
      sendButton.disabled = disabled || selectedChatId <= 0;
    };

    const renderChats = (items) => {
      if (!items.length) {
        chatsList.innerHTML = '<div class="admin-support-empty">Чатов пока нет</div>';
        return;
      }

      chatsList.innerHTML = items
        .map((item) => {
          const chatId = Number.parseInt(String(item?.id || "0"), 10);
          const safeChatId = Number.isFinite(chatId) ? chatId : 0;
          const unread = Number.parseInt(String(item?.adminsUnreadCount || "0"), 10);
          const unreadValue = Number.isFinite(unread) && unread > 0 ? unread : 0;
          const title = sanitizeChatTitle(item?.title) || `Чат ${safeChatId}`;
          const preview = String(item?.lastMessageText || "").trim() || "Нет сообщений";
          const timeText = formatSupportDate(item?.lastMessageAt);
          const activeClass = safeChatId > 0 && safeChatId === selectedChatId ? " is-active" : "";
          const unreadHtml =
            unreadValue > 0
              ? `<span class="admin-support-chat-unread">${escapeHtml(unreadValue > 99 ? "99+" : String(unreadValue))}</span>`
              : "";
          return `
            <button class="admin-support-chat-item${activeClass}" type="button" data-chat-id="${escapeHtml(String(safeChatId))}">
              <div>
                <div class="admin-support-chat-title">${escapeHtml(title)}</div>
                <div class="admin-support-chat-preview">${escapeHtml(preview)}</div>
              </div>
              <div class="admin-support-chat-side">
                <span class="admin-support-chat-time">${escapeHtml(timeText)}</span>
                ${unreadHtml}
              </div>
            </button>
          `;
        })
        .join("");
    };

    const renderMessages = (messages) => {
      if (!messages.length) {
        messagesNode.innerHTML = '<div class="admin-support-empty">Сообщений пока нет</div>';
        return;
      }
      messagesNode.innerHTML = messages
        .map((item) => {
          const role = String(item?.senderRole || "").trim().toLowerCase();
          const rowClass =
            role === "admin"
              ? "admin-support-message admin-support-message--admin"
              : "admin-support-message admin-support-message--user";
          const author = role === "admin" ? "Админ" : "Пользователь";
          const textValue = String(item?.text || "").trim();
          const photoUrl = resolvePhotoUrl(item?.photoUrl);
          const photoHtml = photoUrl
            ? `<img class="admin-support-message-photo" src="${escapeHtml(photoUrl)}" alt="Фото сообщения" data-photo-open="${escapeHtml(photoUrl)}">`
            : "";
          const textHtml = textValue
            ? `<div class="admin-support-message-text">${escapeHtml(textValue)}</div>`
            : "";
          return `
            <article class="${rowClass}">
              <div class="admin-support-message-head">
                <span class="admin-support-message-author">${escapeHtml(author)}</span>
                <span class="admin-support-message-time">${escapeHtml(formatSupportDate(item?.createdAt))}</span>
              </div>
              ${photoHtml}
              ${textHtml}
            </article>
          `;
        })
        .join("");
      messagesNode.scrollTop = messagesNode.scrollHeight;
    };

    const clearReplyPhoto = () => {
      replyPhotoFile = null;
      replyPhotoInput.value = "";
      replyPhotoName.textContent = "Без файла";
      replyPhotoClearButton.hidden = true;
    };

    const loadChatsPage = async (nextPage, nextQuery = query, preserveSelection = true) => {
      const safeQuery = String(nextQuery || "").trim();
      setListLoadingState(true);
      try {
        const data = await adminRequest("support_chats_list", {
          page: nextPage,
          limit: 12,
          query: safeQuery,
        });
        const items = Array.isArray(data?.items) ? data.items : [];
        const safePage = Number.parseInt(data?.page, 10);
        const safeTotalPages = Number.parseInt(data?.totalPages, 10);
        const safeTotal = Number.parseInt(data?.total, 10);
        page = Number.isFinite(safePage) && safePage > 0 ? safePage : 1;
        totalPages = Number.isFinite(safeTotalPages) && safeTotalPages > 0 ? safeTotalPages : 1;
        total = Number.isFinite(safeTotal) && safeTotal >= 0 ? safeTotal : 0;
        query = String(data?.query ?? safeQuery ?? "").trim();
        searchInput.value = query;

        const hasSelectedItem = items.some((item) => Number(item?.id || 0) === selectedChatId);
        if (!preserveSelection || !hasSelectedItem) {
          selectedChatId = 0;
          setSupportPaneMode(false);
          setChatLoadingState(false);
        }

        renderChats(items);
        resultNode.textContent = `Чатов: ${formatNumber(total)}`;
      } catch (error) {
        chatsList.innerHTML = `<div class="admin-support-empty">${escapeHtml(error.message)}</div>`;
        page = 1;
        totalPages = 1;
        total = 0;
        resultNode.textContent = error.message;
      } finally {
        setListLoadingState(false);
      }
    };

    const openChat = async (chatId) => {
      const safeChatId = Number.parseInt(String(chatId || "0"), 10);
      if (!Number.isFinite(safeChatId) || safeChatId <= 0 || loadingChat) {
        return;
      }
      selectedChatId = safeChatId;
      chatsList.querySelectorAll(".admin-support-chat-item").forEach((node) => {
        const rowChatId = Number.parseInt(String(node.getAttribute("data-chat-id") || "0"), 10);
        node.classList.toggle("is-active", Number.isFinite(rowChatId) && rowChatId === selectedChatId);
      });
      setChatLoadingState(true);
      try {
        const data = await adminRequest("support_chat_open", {
          chatId: safeChatId,
          messagesLimit: 180,
        });
        const chat = data?.chat || {};
        const messages = Array.isArray(data?.messages) ? data.messages : [];
        const usernameText = String(chat?.username || "").trim();
        const userIdText = String(chat?.userId || "").trim();
        const defaultTitle = sanitizeChatTitle(chat?.title);
        titleInput.value = String(chat?.customTitle || defaultTitle || "").trim();
        chatMeta.textContent = `Пользователь: ${usernameText ? `@${usernameText}` : "—"} · ID: ${userIdText || "—"} · Чат: ${defaultTitle || `#${safeChatId}`}`;
        setSupportPaneMode(true);
        renderMessages(messages);
        await loadChatsPage(page, query, true);
      } catch (error) {
        setSupportPaneMode(false);
        selectedChatId = 0;
        resultNode.textContent = error.message;
      } finally {
        setChatLoadingState(false);
      }
    };

    const sendReply = async () => {
      if (loadingChat || selectedChatId <= 0) {
        return;
      }
      const text = String(replyInput.value || "").trim();
      if (!text && !(replyPhotoFile instanceof File)) {
        resultNode.textContent = "Введите сообщение или добавьте фото.";
        return;
      }
      setChatLoadingState(true);
      try {
        let imageBase64 = "";
        let imageMime = "";
        if (replyPhotoFile instanceof File) {
          const preparedFile = await compressImageForUpload(replyPhotoFile);
          const dataUrl = await fileToDataUrl(preparedFile);
          imageBase64 = stripDataUrlPrefix(dataUrl);
          imageMime = String(preparedFile.type || "image/jpeg").toLowerCase();
        }

        const data = await adminRequest("support_chat_send", {
          chatId: selectedChatId,
          text,
          imageBase64,
          imageMime,
        });
        const chat = data?.chat || {};
        const messages = Array.isArray(data?.messages) ? data.messages : [];
        titleInput.value = String(chat?.customTitle || chat?.title || "").trim();
        renderMessages(messages);
        replyInput.value = "";
        clearReplyPhoto();
        resultNode.textContent = "Ответ отправлен.";
        await loadChatsPage(page, query, true);
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setChatLoadingState(false);
      }
    };

    const renameCurrentChat = async () => {
      if (loadingChat || selectedChatId <= 0) {
        return;
      }
      const title = String(titleInput.value || "").trim();
      if (!title) {
        resultNode.textContent = "Введите название чата.";
        return;
      }
      setChatLoadingState(true);
      try {
        const data = await adminRequest("support_chat_rename", {
          chatId: selectedChatId,
          title,
        });
        const chat = data?.chat || {};
        titleInput.value = String(chat?.customTitle || chat?.title || title).trim();
        resultNode.textContent = "Название чата обновлено.";
        await loadChatsPage(page, query, true);
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setChatLoadingState(false);
      }
    };

    const deleteCurrentChat = async () => {
      if (loadingChat || selectedChatId <= 0) {
        return;
      }
      if (!window.confirm("Удалить этот чат поддержки?")) {
        return;
      }
      setChatLoadingState(true);
      try {
        await adminRequest("support_chat_delete", {
          chatId: selectedChatId,
        });
        selectedChatId = 0;
        setSupportPaneMode(false);
        replyInput.value = "";
        titleInput.value = "";
        clearReplyPhoto();
        resultNode.textContent = "Чат удалён.";
        await loadChatsPage(1, query, false);
      } catch (error) {
        resultNode.textContent = error.message;
      } finally {
        setChatLoadingState(false);
      }
    };

    searchButton.addEventListener("click", () => {
      const nextQuery = String(searchInput.value || "").trim();
      loadChatsPage(1, nextQuery, false).catch(() => {});
    });

    searchInput.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") {
        return;
      }
      event.preventDefault();
      const nextQuery = String(searchInput.value || "").trim();
      loadChatsPage(1, nextQuery, false).catch(() => {});
    });

    prevButton.addEventListener("click", () => {
      if (loadingList || page <= 1) {
        return;
      }
      loadChatsPage(page - 1, query, true).catch(() => {});
    });

    nextButton.addEventListener("click", () => {
      if (loadingList || page >= totalPages) {
        return;
      }
      loadChatsPage(page + 1, query, true).catch(() => {});
    });

    chatsList.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) {
        return;
      }
      const chatNode = target.closest("[data-chat-id]");
      if (!chatNode) {
        return;
      }
      const chatId = Number.parseInt(String(chatNode.getAttribute("data-chat-id") || "0"), 10);
      if (!Number.isFinite(chatId) || chatId <= 0) {
        return;
      }
      openChat(chatId).catch(() => {});
    });

    backButton.addEventListener("click", () => {
      selectedChatId = 0;
      setSupportPaneMode(false);
      setChatLoadingState(false);
      clearReplyPhoto();
      loadChatsPage(page, query, false).catch(() => {});
    });

    messagesNode.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) {
        return;
      }
      const image = target.closest("[data-photo-open]");
      if (!image) {
        return;
      }
      const url = String(image.getAttribute("data-photo-open") || "").trim();
      if (!url) {
        return;
      }
      openSupportPhoto(url);
    });

    replyPhotoInput.addEventListener("change", () => {
      replyPhotoFile = replyPhotoInput.files?.[0] || null;
      replyPhotoName.textContent = replyPhotoFile ? replyPhotoFile.name : "Без файла";
      replyPhotoClearButton.hidden = !(replyPhotoFile instanceof File);
    });

    replyPhotoClearButton.addEventListener("click", (event) => {
      event.preventDefault();
      clearReplyPhoto();
    });

    sendButton.addEventListener("click", () => {
      sendReply().catch(() => {});
    });

    replyInput.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" || event.shiftKey) {
        return;
      }
      event.preventDefault();
      sendReply().catch(() => {});
    });

    renameButton.addEventListener("click", () => {
      renameCurrentChat().catch(() => {});
    });

    deleteButton.addEventListener("click", () => {
      deleteCurrentChat().catch(() => {});
    });

    updatePagination();
    setChatLoadingState(false);
    setSupportPaneMode(false);
    await loadChatsPage(1, "", false);
  };

  const renderLogsSection = async () => {
    adminSectionContent.innerHTML = `
      <div class="admin-split admin-split--search">
        <div class="admin-search-wrap">
          <input class="admin-input admin-input--search" id="adminLogsRefreshInput" type="text" value="Журнал действий" readonly>
          <button class="admin-search-button admin-search-button--update" id="adminLogsRefreshButton" type="button" aria-label="Обновить журнал">
            <img src="update.png" alt="" aria-hidden="true">
          </button>
        </div>
      </div>
      <div class="admin-table-wrap">
        <table class="admin-table">
          <thead><tr><th>Дата</th><th>Админ</th><th>Действие</th><th>Статус</th><th>Детали</th></tr></thead>
          <tbody id="adminLogsTableBody"><tr><td colspan="5">Загрузка...</td></tr></tbody>
        </table>
      </div>
      <div class="admin-pagination" id="adminLogsPagination">
        <button class="admin-action-button" id="adminLogsPrevButton" type="button">← Назад</button>
        <div class="admin-page-indicator" id="adminLogsPageInfo">Страница 1 / 1</div>
        <button class="admin-action-button" id="adminLogsNextButton" type="button">Вперёд →</button>
      </div>
      <div class="admin-result-card" id="adminLogsResult"></div>
    `;

    const refreshButton = document.getElementById("adminLogsRefreshButton");
    const tableBody = document.getElementById("adminLogsTableBody");
    const prevButton = document.getElementById("adminLogsPrevButton");
    const nextButton = document.getElementById("adminLogsNextButton");
    const pageInfo = document.getElementById("adminLogsPageInfo");
    const resultNode = document.getElementById("adminLogsResult");
    if (!refreshButton || !tableBody || !prevButton || !nextButton || !pageInfo || !resultNode) {
      return;
    }

    let page = 1;
    let totalPages = 1;
    let loading = false;

    const formatLogDate = (rawValue) => {
      const parsed = new Date(String(rawValue || ""));
      if (Number.isNaN(parsed.getTime())) {
        return "—";
      }
      return parsed.toLocaleString();
    };

    const updatePagination = () => {
      pageInfo.textContent = `Страница ${page} / ${totalPages}`;
      prevButton.disabled = loading || page <= 1;
      nextButton.disabled = loading || page >= totalPages;
    };

    const setLoadingState = (disabled) => {
      loading = disabled;
      refreshButton.disabled = disabled;
      updatePagination();
    };

    const renderRows = (items) => {
      if (!items.length) {
        tableBody.innerHTML = '<tr><td colspan="5">Записей пока нет</td></tr>';
        return;
      }

      tableBody.innerHTML = items
        .map((item) => {
          const usernameRaw = String(item?.adminUsername || "").trim().replace(/^@+/, "");
          const usernameText = usernameRaw ? `@${escapeHtml(usernameRaw)}` : "—";
          const adminLabel = `${escapeHtml(String(item?.adminUserId || "—"))} ${usernameText}`;
          const actionText = escapeHtml(String(item?.action || "—"));
          const detailsText = escapeHtml(String(item?.details || item?.errorText || "—"));
          const isError = String(item?.status || "").toLowerCase() === "error";
          const statusClass = isError ? "admin-status-badge--error" : "admin-status-badge--success";
          const statusLabel = isError ? "Ошибка" : "Успех";
          return `<tr><td>${escapeHtml(formatLogDate(item?.createdAt))}</td><td>${adminLabel}</td><td>${actionText}</td><td><span class="admin-status-badge ${statusClass}">${statusLabel}</span></td><td>${detailsText}</td></tr>`;
        })
        .join("");
    };

    const loadLogsPage = async (nextPage) => {
      setLoadingState(true);
      tableBody.innerHTML = '<tr><td colspan="5">Загрузка...</td></tr>';
      try {
        const data = await adminRequest("admin_logs_list", { page: nextPage, limit: 15 });
        const items = Array.isArray(data.items) ? data.items : [];
        const safePage = Number.parseInt(data.page, 10);
        const safeTotalPages = Number.parseInt(data.totalPages, 10);
        const safeTotal = Number.parseInt(data.total, 10);

        page = Number.isFinite(safePage) && safePage > 0 ? safePage : 1;
        totalPages = Number.isFinite(safeTotalPages) && safeTotalPages > 0 ? safeTotalPages : 1;

        renderRows(items);
        resultNode.textContent = `Записей: ${formatNumber(Number.isFinite(safeTotal) ? safeTotal : 0)}`;
      } catch (error) {
        tableBody.innerHTML = `<tr><td colspan="5">${escapeHtml(error.message)}</td></tr>`;
        page = 1;
        totalPages = 1;
        resultNode.textContent = error.message;
      } finally {
        setLoadingState(false);
      }
    };

    refreshButton.addEventListener("click", () => {
      loadLogsPage(page).catch(() => {});
    });

    prevButton.addEventListener("click", () => {
      if (loading || page <= 1) {
        return;
      }
      loadLogsPage(page - 1).catch(() => {});
    });

    nextButton.addEventListener("click", () => {
      if (loading || page >= totalPages) {
        return;
      }
      loadLogsPage(page + 1).catch(() => {});
    });

    updatePagination();
    await loadLogsPage(1);
  };

  sectionRenderers = {
    stats: renderStatsSection,
    promos: renderPromosSection,
    referrals: renderReferralsSection,
    stars: renderStarsSection,
    payment: renderPaymentSection,
    broadcast: renderBroadcastSection,
    "find-user": renderFindUserSection,
    "add-stars": renderAddStarsSection,
    admins: renderAdminsSection,
    support: renderSupportSection,
    operations: renderOperationsSection,
    logs: renderLogsSection,
  };

  adminHubCloseButton?.addEventListener("click", (event) => {
    event.preventDefault();
    navigateToScreen("profile");
  });

  adminSectionCloseButton?.addEventListener("click", (event) => {
    event.preventDefault();
    navigateToScreen("admin");
  });

  sectionButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const sectionKey = button.dataset.adminSection || "";
      openSection(sectionKey).catch((error) => {
        showAlert(error.message);
      });
    });
  });
})();

(() => {
  const isEditableTarget = (node) => {
    if (!(node instanceof Element)) {
      return false;
    }
    return Boolean(
      node.closest("input, textarea, select, [contenteditable=''], [contenteditable='true'], label")
    );
  };

  const isEditableContext = (event) => {
    const target = event?.target;
    if (isEditableTarget(target)) {
      return true;
    }
    return isEditableTarget(document.activeElement);
  };

  const block = (event) => {
    if (isEditableContext(event)) {
      return;
    }
    event.preventDefault();
  };

  document.addEventListener("copy", block);
  document.addEventListener("cut", block);
  document.addEventListener("contextmenu", block);
  document.addEventListener("dragstart", block);

  document.addEventListener("selectstart", (event) => {
    if (isEditableContext(event)) {
      return;
    }

    event.preventDefault();
  });

  document.addEventListener("keydown", (event) => {
    if (!(event.ctrlKey || event.metaKey)) {
      return;
    }

    const key = event.key.toLowerCase();
    if (key === "c" || key === "x") {
      if (isEditableContext(event)) {
        return;
      }
      event.preventDefault();
    }
  });
})();

