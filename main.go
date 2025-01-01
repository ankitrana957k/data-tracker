package main

import (
	"fmt"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"

	"github.com/krogertechnology/data-tracker/models"
	"github.com/krogertechnology/data-tracker/service"
	"github.com/krogertechnology/data-tracker/utils"
)

func main() {
	clearMsgAfter := 10

	a := app.NewWithID("com.eventhub.datatracker")
	window := a.NewWindow("Data Tracker v0.1")

	// Tabs
	tabBar := container.NewAppTabs()
	tabBar.SetTabLocation(container.TabLocationLeading)

	widgetMap := make(map[string]*widget.Label, 0)
	creationChan := make(chan models.Config)

	gui := GUI{widgetMap, tabBar, window}

	form := gui.CreateKafkaConfigForm(creationChan)
	formContainer := container.New(layout.NewCenterLayout(), form)

	tabHeader := container.NewTabItemWithIcon("Add Eventhub", theme.ContentAddIcon(), formContainer)
	tabBar.Append(tabHeader)

	go gui.UpdateUIWithNewConnection(creationChan, clearMsgAfter)

	window.SetPadded(true)
	window.Resize(fyne.NewSize(770, 750))
	window.SetContent(tabBar)
	window.CenterOnScreen()

	window.ShowAndRun()
}

type GUI struct {
	WidgetMap map[string]*widget.Label
	TabBar    *container.AppTabs
	Window    fyne.Window
}

func (g *GUI) UpdateUIWithNewConnection(creationChan chan models.Config, clearMsgAfter int) {
	for config := range creationChan {
		kafkaOBJ := service.NewKafkaObj(config, clearMsgAfter)

		client, err := kafkaOBJ.SetupEventhub()
		if err != nil {
			dialog.ShowError(err, g.Window)
			continue
		}

		if err := g.AddEventhubUI(kafkaOBJ); err != nil {
			dialog.ShowError(err, g.Window)
			continue
		}

		go func() {
			err := kafkaOBJ.ReadFromConsumerGroup(client)
			if err != nil {
				dialog.ShowError(err, g.Window)
			}
		}()

		go func() {
			err := kafkaOBJ.StartListening(g.WidgetMap)
			if err != nil {
				dialog.ShowError(err, g.Window)
			}
		}()
	}
}

func (g *GUI) AddEventhubUI(k *service.KafkaOBJ) error {
	for _, topic := range k.Configs.TOPICS {
		textWidget := utils.CreateTextWidget()

		scrollView := container.NewVScroll(textWidget)
		tabItem := container.NewTabItemWithIcon(topic, theme.ComputerIcon(), scrollView)

		defaultText := fmt.Sprintf("Establishing a connection with %s\n", topic)
		textWidget.SetText(defaultText)

		_, ok := k.DataChannel[topic]
		if ok {
			return fmt.Errorf("same topic %s can't be added twice", topic)
		}

		k.DataChannel[topic] = make(chan models.Message)

		g.TabBar.Append(tabItem)
		g.WidgetMap[topic] = textWidget
	}

	return nil
}

func (g *GUI) CreateKafkaConfigForm(creationChan chan models.Config) *fyne.Container {
	form := widget.NewForm()

	kafkaHostField := utils.CreateEntryWidget("Enter Your Kafka Host Name", true, false)
	kafkaTopicField := utils.CreateEntryWidget("Enter Your Kafka Topic Name", true, false)
	kafkaConsumerIdField := utils.CreateEntryWidget("Enter Your Kafka Consumer Group ID", true, false)
	kafkaConsumerOffsetField := utils.CreateEntryWidget("Enter Your Consumer Offset eg: Latest, Oldest", true, false)
	kafkaSASLUserField := utils.CreateEntryWidget("Enter Your SASL Username", false, false)
	kafkaSASLPasswordField := utils.CreateEntryWidget("Enter Your SASL Password", false, true)
	azureAudienceField := utils.CreateEntryWidget("Enter Your Azure Audience", false, false)
	azureTenantIdField := utils.CreateEntryWidget("Enter Your Azure TenantID", false, false)
	azureApplicationIdField := utils.CreateEntryWidget("Enter Your Azure ApplicationID", false, false)
	azureApplicationSecretField := utils.CreateEntryWidget("Enter Your Azure Application Secret", false, true)
	avroSchemaUrlField := utils.CreateEntryWidget("Enter Your Avro Schema URL", false, false)
	avroSchemaVersionField := utils.CreateEntryWidget("Enter Your Avro Schema Version", false, false)

	// default value
	kafkaSASLUserField.SetText("$ConnectionString")

	saslMechanismOptions := []string{"PLAINTEXT", "SASL/PLAIN", "OUTHBEARER"}
	saslMechanism := widget.NewRadioGroup(saslMechanismOptions, func(selected string) {
		switch selected {
		case "OUTHBEARER":
			kafkaSASLUserField.Disable()
			kafkaSASLPasswordField.Disable()
			azureAudienceField.Enable()
			azureTenantIdField.Enable()
			azureApplicationIdField.Enable()
			azureApplicationSecretField.Enable()
			form.Refresh()

		case "PLAINTEXT":
			kafkaSASLUserField.Disable()
			kafkaSASLPasswordField.Disable()
			azureAudienceField.Disable()
			azureTenantIdField.Disable()
			azureApplicationIdField.Disable()
			azureApplicationSecretField.Disable()
			form.Refresh()

		case "SASL/PLAIN":
			kafkaSASLUserField.Enable()
			kafkaSASLPasswordField.Enable()
			azureAudienceField.Disable()
			azureTenantIdField.Disable()
			azureApplicationIdField.Disable()
			azureApplicationSecretField.Disable()
			form.Refresh()
		}

	})
	saslMechanism.Horizontal = true

	dataFormats := []string{"JSON", "AVRO"}
	dataFormatRadio := widget.NewRadioGroup(dataFormats, func(selected string) {
		switch selected {
		case "JSON":
			avroSchemaUrlField.Disable()
			avroSchemaVersionField.Disable()
			form.Refresh()

		case "AVRO":
			avroSchemaUrlField.Enable()
			avroSchemaVersionField.Enable()
			form.Refresh()
		}
	})

	// Set up form submit button and collect the data
	submitButton := widget.NewButton("Connect", func() {
		if kafkaHostField.Text == "" || kafkaTopicField.Text == "" || kafkaConsumerIdField.Text == "" || kafkaConsumerOffsetField.Text == "" {
			return
		}

		if saslMechanism.Selected == "OUTHBEARER" {
			if azureAudienceField.Text == "" || azureTenantIdField.Text == "" || azureApplicationIdField.Text == "" || azureApplicationSecretField.Text == "" {
				return
			}
		}

		if saslMechanism.Selected == "SASL/PLAIN" {
			if kafkaSASLUserField.Text == "" || kafkaSASLPasswordField.Text == "" {
				return
			}
		}

		if dataFormatRadio.Selected == "AVRO" {
			if avroSchemaUrlField.Text == "" || avroSchemaVersionField.Text == "" {
				return
			}
		}

		kafkaConfig := models.Config{
			KAFKA_HOSTS:             kafkaHostField.Text,
			KAFKA_TOPIC:             kafkaTopicField.Text,
			KAFKA_CONSUMER_GROUP_ID: kafkaConsumerIdField.Text,
			KAFKA_CONSUMER_OFFSET:   kafkaConsumerOffsetField.Text,
			KAFKA_SASL_MECHANISM:    saslMechanism.Selected,
			KAFKA_SASL_USER:         kafkaSASLUserField.Text,
			KAFKA_SASL_PASS:         kafkaSASLPasswordField.Text,
			AZURE_CONFIGS: &models.AzureConfig{
				AAD_AUDIENCE:           azureAudienceField.Text,
				AAD_TENANT_ID:          azureTenantIdField.Text,
				AAD_APPLICATION_ID:     azureApplicationIdField.Text,
				AAD_APPLICATION_SECRET: azureApplicationSecretField.Text,
			},
			AVRO_CONFIGS: &models.AvroConfig{
				SCHEMA_URL:     avroSchemaUrlField.Text,
				SCHEMA_VERSION: avroSchemaVersionField.Text,
			},
		}

		if kafkaConfig.KAFKA_SASL_MECHANISM == "SASL/PLAIN" {
			kafkaConfig.KAFKA_SASL_MECHANISM = "PLAIN"
		}

		creationChan <- kafkaConfig

		// Resetting the fields
		kafkaHostField.SetText("")
		kafkaTopicField.SetText("")
		kafkaConsumerIdField.SetText("")
		kafkaConsumerOffsetField.SetText("")
		saslMechanism.SetSelected("")
		kafkaSASLUserField.SetText("")
		kafkaSASLPasswordField.SetText("")
		azureAudienceField.SetText("")
		azureTenantIdField.SetText("")
		azureApplicationIdField.SetText("")
		azureApplicationSecretField.SetText("")
		dataFormatRadio.SetSelected("")
		avroSchemaUrlField.SetText("")
		avroSchemaVersionField.SetText("")
	})

	dataFormatRadio.Horizontal = true

	loadJson := widget.NewButton("Import From JSON", func() {
		fileDialog := dialog.NewFileOpen(
			func(r fyne.URIReadCloser, err error) {
				if err != nil {
					fmt.Println("Error reading file:", err)
					return
				}

				if r != nil {
					configs, err := utils.ProcessJSONFile(r.URI().Path())
					if err != nil {
						fmt.Println("Error processing JSON file:", err)
						return
					}

					for i := range configs {
						if configs[i].KAFKA_SASL_MECHANISM == "SASL/PLAIN" {
							configs[i].KAFKA_SASL_MECHANISM = "PLAIN"
						}

						creationChan <- configs[i]
					}

					r.Close()
				}
			},
			g.Window,
		)

		// Open the dialog
		fileDialog.Show()

	})

	kafkaConfigFields := []*widget.FormItem{
		{
			Text:   "KAFKA HOST",
			Widget: kafkaHostField,
		},
		{
			Text:   "KAFKA TOPIC",
			Widget: kafkaTopicField,
		},
		{
			Text:   "KAFKA CONSUMER GROUP ID",
			Widget: kafkaConsumerIdField,
		},
		{
			Text:   "KAFKA CONSUMER OFFSET",
			Widget: kafkaConsumerOffsetField,
		},
		{
			Text:     "KAFKA SASL MECHANISM",
			HintText: "Select Your Preferred Mechanism (Mandatory Field)",
			Widget:   saslMechanism,
		},
		{
			Text:   "KAFKA SASL USERNAME",
			Widget: kafkaSASLUserField,
		},
		{
			Text:   "KAFKA SASL PASSWORD",
			Widget: kafkaSASLPasswordField,
		},
		{
			Text:   "AZURE AUDIENCE",
			Widget: azureAudienceField,
		},
		{
			Text:   "AZURE TENANT ID",
			Widget: azureTenantIdField,
		},
		{
			Text:   "AZURE APPLICATION ID",
			Widget: azureApplicationIdField,
		},
		{
			Text:   "AZURE APPLICATION SECRET",
			Widget: azureApplicationSecretField,
		},
		{
			Text:     "DATA FORMAT TYPE",
			HintText: "Select Receiving Data Format (Mandatory Field)",
			Widget:   dataFormatRadio,
		},
		{
			Text:   "AVRO SCHEMA URL",
			Widget: avroSchemaUrlField,
		},
		{
			Text:   "AVRO SCHEMA VERSION",
			Widget: avroSchemaVersionField,
		},
		{
			Text:   "",
			Widget: submitButton,
		},
		{
			Text:   "",
			Widget: loadJson,
		},
	}
	form.Items = append(form.Items, kafkaConfigFields...)

	title := widget.NewLabelWithStyle("EVENTHUB CONFIGURATIONS", fyne.TextAlignCenter, fyne.TextStyle{Bold: true})
	c := container.NewVBox(title, layout.NewSpacer(), container.NewHBox(form, layout.NewSpacer()), layout.NewSpacer())

	return c
}
