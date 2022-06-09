package cls

import "time"

func GetFirstAndLastOfMonth(day time.Time) (time.Time, time.Time) {
	currentYear, currentMonth, _ := day.Date()
	currentLocation := day.Location()

	firstOfMonth := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, currentLocation)
	lastOfMonth := firstOfMonth.AddDate(0, 1, -1)

	return firstOfMonth, lastOfMonth
}

func GetFirstOfMonth(day time.Time) time.Time {
	currentYear, currentMonth, _ := day.Date()
	currentLocation := day.Location()

	return time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, currentLocation)
}

func GetLastOfMonth(day time.Time) time.Time {
	currentYear, currentMonth, _ := day.Date()
	currentLocation := day.Location()

	firstOfMonth := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, currentLocation)

	return firstOfMonth.AddDate(0, 1, -1)
}

func GetFirstOfWeek(day time.Time) time.Time {
	weekday := int(day.Weekday())

	return day.AddDate(0, 0, -weekday)
}

func GetEndOfWeek(day time.Time) time.Time {
	weekday := int(day.Weekday())

	return day.AddDate(0, 0, (6 - weekday))
}
