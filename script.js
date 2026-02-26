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
    const normalized = clamp(clientX - bounds.left, 0, bounds.width);
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

  const setActiveScreen = (targetScreen) => {
    const knownScreen = screens.some((screen) => screen.dataset.screen === targetScreen);
    const nextScreen = knownScreen ? targetScreen : "home";

    screens.forEach((screen) => {
      const isActive = screen.dataset.screen === nextScreen;
      screen.classList.toggle("is-active", isActive);
      screen.setAttribute("aria-hidden", isActive ? "false" : "true");
    });

    setSelectedButton(nextScreen);
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
    setOrbPosition(event.clientX);
    setButtonHighlightByX(event.clientX);
    dock.setPointerCapture?.(event.pointerId);
    event.preventDefault();
  });

  dock.addEventListener("pointermove", (event) => {
    if (!isDragging || (activePointerId !== null && event.pointerId !== activePointerId)) {
      return;
    }

    setOrbPosition(event.clientX);
    setButtonHighlightByX(event.clientX);
  });

  dock.addEventListener("pointerup", (event) => {
    if (activePointerId !== null && event.pointerId !== activePointerId) {
      return;
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
  const grids = document.querySelectorAll(".offer-grid");
  if (!grids.length) {
    return;
  }

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

      const sourceCard = card || cards[0] || null;
      if (!sourceCard) {
        priceUsdNode.textContent = "—";
        priceRubNode.textContent = "—";
        return;
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
      syncPriceRow(activeCard);

      if (!quantityInput) {
        return;
      }

      const nextValue = activeCard ? getCardAmount(activeCard) : "";
      if (quantityInput.value !== nextValue) {
        quantityInput.value = nextValue;
      }
    };

    const initialActiveCard = grid.querySelector(".offer-card.is-active");
    if (initialActiveCard) {
      setActiveCard(initialActiveCard);
    }
    syncInputWithActiveCard();

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
  if (!usernameInputs.length) {
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

    if (target.closest(".form-input-wrap")) {
      return;
    }

    activeElement.blur();
  };

  document.addEventListener("pointerdown", dismissKeyboardIfNeeded);
  document.addEventListener("touchstart", dismissKeyboardIfNeeded, { passive: true });
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
  if (!storePage || !profilePage) {
    return;
  }

  const historyStorageKey = "starslix_purchase_history";

  const setProfilePageState = (isProfilePage) => {
    document.body.classList.toggle("is-profile-page", isProfilePage);
    if (avatarToggle) {
      avatarToggle.setAttribute("aria-expanded", isProfilePage ? "true" : "false");
    }
    profilePage.setAttribute("aria-hidden", isProfilePage ? "false" : "true");
    storePage.setAttribute("aria-hidden", isProfilePage ? "true" : "false");
  };

  const formatHistoryDate = (isoValue) => {
    const parsedDate = new Date(isoValue);
    if (Number.isNaN(parsedDate.getTime())) {
      return "";
    }
    return parsedDate.toLocaleString();
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

  // Временные тестовые данные истории покупок.
  const now = Date.now();
  const testPurchaseHistory = [
    {
      main: "Звёзды: 100",
      price: "1.96$/150₽",
      createdAt: new Date(now - 2 * 24 * 60 * 60 * 1000).toISOString(),
    },
    {
      main: "Премиум: 6 мес.",
      price: "19.44$/1490₽",
      createdAt: new Date(now - 24 * 60 * 60 * 1000).toISOString(),
    },
    {
      main: "TON: 50",
      price: "50 TON",
      createdAt: new Date(now - 6 * 60 * 60 * 1000).toISOString(),
    },
  ];

  let historyItems = testPurchaseHistory.slice();
  saveHistory(historyItems);

  const renderHistory = () => {
    if (!profileHistoryList || !profileHistoryEmpty) {
      return;
    }

    profileHistoryList.querySelectorAll(".profile-history-item").forEach((node) => node.remove());

    if (!historyItems.length) {
      profileHistoryEmpty.hidden = false;
      return;
    }

    profileHistoryEmpty.hidden = true;

    historyItems.forEach((item) => {
      const row = document.createElement("div");
      row.className = "profile-history-item";

      const main = document.createElement("span");
      main.className = "profile-history-main";
      main.textContent = item.main || "—";

      const price = document.createElement("span");
      price.className = "profile-history-price";
      price.textContent = item.price || "—";

      const date = document.createElement("span");
      date.className = "profile-history-date";
      date.textContent = formatHistoryDate(item.createdAt);

      row.append(main, price, date);
      profileHistoryList.appendChild(row);
    });
  };

  const addCurrentOrderToHistory = () => {
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
    const sourceTab = document.querySelector(`.panel-tab[data-panel="${viewName}"]`);
    const sectionName = sourceTab?.getAttribute("aria-label")?.trim() || viewName;
    const amountText = activeCard.querySelector(".offer-card-amount")?.textContent?.trim() || "";
    const priceText = activeCard.querySelector(".offer-card-price")?.textContent?.trim() || "—";

    historyItems.unshift({
      main: amountText ? `${sectionName}: ${amountText}` : sectionName || "—",
      price: priceText,
      createdAt: new Date().toISOString(),
    });
    historyItems = historyItems.slice(0, 20);

    saveHistory(historyItems);
    renderHistory();
  };

  const telegramUser = window.Telegram?.WebApp?.initDataUnsafe?.user;
  const firstName = telegramUser?.first_name?.trim() || "";
  const lastName = telegramUser?.last_name?.trim() || "";
  const usernameRaw = telegramUser?.username?.trim() || "";
  const telegramId = telegramUser?.id ? String(telegramUser.id) : "";
  const username = usernameRaw ? `@${usernameRaw.replace(/^@+/, "")}` : "";

  if (profileUsername) {
    profileUsername.textContent = username || "—";
  }
  if (profileTelegramId) {
    profileTelegramId.textContent = telegramId || "—";
  }

  if (profileFullname) {
    const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();
    if (fullName) {
      profileFullname.textContent = fullName;
      profileFullname.dataset.empty = "false";
    }
  }

  if (profileAvatar && typeof telegramUser?.photo_url === "string" && telegramUser.photo_url) {
    profileAvatar.src = telegramUser.photo_url;
  }

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

  document.querySelectorAll(".order-submit-button").forEach((button) => {
    button.addEventListener("click", addCurrentOrderToHistory);
  });

  setProfilePageState(false);
  renderHistory();
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
      heroMobileLine1: "Получите",
      heroMobileLine2Prefix: "свои ",
      heroStars: "звёзды",
      heroMobileLine3: "здесь!",
      heroWideLine1: "Получите свои",
      heroWideLine2Suffix: "здесь!",
      heroSubtitleLine1: "Покупайте звёзды быстро и анонимно.",
      heroSubtitleLine2: "Никаких KYC верификации и риска возврата.",
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
      profileHistoryTitle: "История покупок",
      profileHistoryEmpty: "Покупок пока нет",
    },
    en: {
      loaderAria: "Loading",
      profileButtonAria: "Open profile page",
      telegramButton: "Telegram bot",
      languageButton: "Switch language",
      languageShort: "EN",
      languageOptionRu: "Russian",
      languageOptionEn: "English",
      heroMobileLine1: "Get",
      heroMobileLine2Prefix: "your ",
      heroStars: "stars",
      heroMobileLine3: "here!",
      heroWideLine1: "Get your",
      heroWideLine2Suffix: "here!",
      heroSubtitleLine1: "Buy stars quickly and anonymously.",
      heroSubtitleLine2: "No KYC verification and no chargeback risk.",
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
      profileHistoryTitle: "Purchase history",
      profileHistoryEmpty: "No purchases yet",
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

  const setHeroSecondLines = (dict) => {
    const mobileLine2 = document.querySelector(
      ".hero-title-layout-mobile .hero-title-line:nth-child(2)"
    );
    if (mobileLine2) {
      mobileLine2.innerHTML = `${dict.heroMobileLine2Prefix}<span class="hero-title-accent">${dict.heroStars}</span>`;
    }

    const wideLine2 = document.querySelector(
      ".hero-title-layout-wide .hero-title-line:nth-child(2)"
    );
    if (wideLine2) {
      wideLine2.innerHTML = `<span class="hero-title-accent">${dict.heroStars}</span> ${dict.heroWideLine2Suffix}`;
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

    setText(".hero-title-layout-mobile .hero-title-line:nth-child(1)", dict.heroMobileLine1);
    setHeroSecondLines(dict);
    setText(".hero-title-layout-mobile .hero-title-line:nth-child(3)", dict.heroMobileLine3);
    setText(".hero-title-layout-wide .hero-title-line:nth-child(1)", dict.heroWideLine1);

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
    setText("#profileHistoryTitle", dict.profileHistoryTitle);
    setText("#profileHistoryEmpty", dict.profileHistoryEmpty);

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
  const block = (event) => {
    event.preventDefault();
  };

  document.addEventListener("copy", block);
  document.addEventListener("cut", block);
  document.addEventListener("contextmenu", block);
  document.addEventListener("dragstart", block);

  document.addEventListener("selectstart", (event) => {
    const target = event.target;
    if (target instanceof Element && target.closest("input, textarea")) {
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
      event.preventDefault();
    }
  });
})();

