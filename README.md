# 🪄 paramEXT - Enhanced SyncShare

![Chrome Web Store](https://img.shields.io/badge/Chrome%20Web%20Store-Available-brightgreen?style=flat-square)
![Manifest Version](https://img.shields.io/badge/Manifest%20V3-Compatible-blue?style=flat-square)
![Version](https://img.shields.io/badge/Version-2.9.0-orange?style=flat-square)
![License](https://img.shields.io/badge/License-MIT%20with%20Attribution-green?style=flat-square)

> **paramEXT** — это расширенная версия популярного расширения SyncShare с дополнительными режимами автоматизации и улучшениями.

---

## ⚡ Возможности

- 🪄 **Режим Wand** — классический режим с кнопкой волшебной палочки
- 🔄 **Режим Auto-Insert** — автоматическое заполнение ответов на основе статистики
- ⏭️ **Режим Auto-Solve** — автоматическое решение всего теста с переходом на следующую страницу

---

## 🚀 Установка

### Способ 1: Через Chrome Web Store (оригинальный SyncShare)

Если вам нужна оригинальная версия SyncShare:
👉 [Установить SyncShare](https://chromewebstore.google.com/detail/syncshare/lngijbnmdkejbgnkakeiapeppbpaapib?hl=ru&utm_source=ext_sidebar)

### Способ 2: Установка paramEXT с локального репозитория

#### Шаг 1: Клонируйте репозиторий

```bash
git clone https://github.com/KOSFin/paramEXT-from-syncshare.git
cd paramEXT
```

Или просто скачайте архив и распакуйте его в папку:
```
📁 paramEXT/
  ├── manifest.json
  ├── js/
  ├── css/
  ├── html/
  ├── icons/
  └── _locales/
```

#### Шаг 2: Откройте страницу расширений Chrome

1. Откройте Chrome и перейдите по адресу: **`chrome://extensions/`**
2. В правом верхнем углу включите **"Режим разработчика"**

#### Шаг 3: Загрузите расширение

1. Нажмите кнопку **"Загрузить распакованное расширение"**
2. Выберите папку с paramEXT
3. Готово! 🎉

---

## ⚠️ Важная информация

### Замена оригинального SyncShare

> **paramEXT** автоматически заменит оригинальное расширение SyncShare, если оно установлено. Это происходит потому, что оба расширения используют одинаковые ключи Chrome.

**Это нормально и даже хорошо!** 🎯

paramEXT **повторяет весь функционал оригинального SyncShare** и добавляет:
- ✨ Автоматическое заполнение ответов (Auto-Insert)
- ⏭️ Полностью автоматическое решение тестов (Auto-Solve)
- 🔧 Улучшенный интерфейс с тремя режимами
- 🛡️ Лучшая обработка ошибок и исключений
- 📊 Использование статистики вместо рекомендаций

**Если вы хотите вернуться на оригинальный SyncShare:**
1. Удалите paramEXT из `chrome://extensions/`
2. Установите оригинальный SyncShare из [Chrome Web Store](https://chromewebstore.google.com/detail/syncshare/lngijbnmdkejbgnkakeiapeppbpaapib?hl=ru&utm_source=ext_sidebar)

---

## 📖 Руководство пользователя

### Режимы

#### 🪄 Wand Mode (По умолчанию)
- Показывает кнопку волшебной палочки рядом с каждым вопросом
- Нажмите на кнопку, чтобы увидеть варианты ответов
- Выберите нужный ответ из меню

#### 🔄 Auto-Insert Mode
- Автоматически заполняет ответы на основе **статистики** (самых часто встречающихся ответов)
- Не требует никаких действий — просто откройте тест
- Идеально для быстрого решения

#### ⏭️ Auto-Solve Mode
- Автоматически решает весь тест
- После решения каждого вопроса нажимает кнопку "Следующая страница"
- Нажмите **"Начать"** в расширении, чтобы запустить
- Нажмите **"Остановить"**, чтобы прервать процесс

---

<div align="center">

**Made with ❤️ by paramEXT contributors**

[Оригинальный SyncShare](https://chromewebstore.google.com/detail/syncshare/lngijbnmdkejbgnkakeiapeppbpaapib?hl=ru&utm_source=ext_sidebar) • [Документация](#документация) • [Лицензия](#лицензия)

</div>
