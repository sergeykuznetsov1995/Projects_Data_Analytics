# Автоматизация отчетности. Чат-бот Telegram

## Задачи
- Написать скрипт для сборки и отправки ежедневного отчета по ленте новостей. Отчет должен содержать текст с информацией о значениях ключевых метрик за предыдущий день и графики со значениями метрик за предыдущие 7 дней. Метрики для отслеживания: DAU, количество просмотров, количество лайков, CTR;
- Написать скрипт для сборки и отправки ежедневного отчета по всему приложению. Отчет должен содержать текст с информацией о значениях ключевых метрик за предыдущий день и графики со значениями метрик за предыдущие 7 дней. Метрики для отслеживания: DAU по ленте, DAU по мессенджеру, количество сообщений, количество диалогов за 7 дней.
- Автоматизацию отчета выполнить в связке чат-бот Telegram + GitLab CI/CD. В CI/CD задать расписание для сборки отправки отчетов ежедневно в 11:00.

## Результаты исследования
Произведена автоматизация отчетности для ленты новостей и мессенджера. Отчет приходит ежедневно в 11:00 и содержит всю необходимую информацию.

## Использованные библиотеки
- *pandas*
- *numpy*
- *matplotlib.pyplot*
- *seaborn*
- *telegram*

## Статус проекта
Завершен.
