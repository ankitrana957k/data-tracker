package utils

import (
	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/widget"
)

func CreateTextWidget() *widget.Label {
	w := widget.NewLabelWithStyle("", fyne.TextAlignLeading, fyne.TextStyle{Italic: true, Bold: true})
	w.Wrapping = fyne.TextWrapBreak

	return w
}

func CreateEntryWidget(placeHolder string, enabled bool, protected bool) *widget.Entry {
	var e *widget.Entry

	if protected {
		e = widget.NewPasswordEntry()
	} else {
		e = widget.NewEntry()
	}

	e.SetPlaceHolder(placeHolder)

	if !enabled {
		e.Disable()
	}

	return e
}
