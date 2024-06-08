# Generated by Django 4.2.6 on 2024-06-08 00:00

import uuid

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Account",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("account_name", models.CharField(max_length=255, unique=True)),
                (
                    "tier",
                    models.CharField(
                        choices=[
                            ("community", "community"),
                            ("team", "team"),
                            ("enterprise", "enterprise"),
                        ],
                        max_length=20,
                    ),
                ),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="Team",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("name", models.CharField(max_length=1024, unique=True)),
                (
                    "auth_provider_type",
                    models.CharField(
                        choices=[
                            ("propel_auth", "propel_auth"),
                            ("public", "public"),
                            ("test", "test"),
                        ],
                        max_length=20,
                    ),
                ),
                ("auth_provider_organization_id", models.CharField(max_length=255)),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="User",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("email", models.EmailField(max_length=254, unique=True)),
                ("first_name", models.TextField()),
                ("last_name", models.TextField()),
                (
                    "auth_provider_type",
                    models.CharField(
                        choices=[
                            ("propel_auth", "propel_auth"),
                            ("public", "public"),
                            ("test", "test"),
                        ],
                        max_length=20,
                    ),
                ),
                ("auth_provider_user_id", models.CharField(max_length=255)),
                ("salt", models.UUIDField(default=uuid.uuid4)),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="UserAccountMembership",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "role",
                    models.CharField(
                        choices=[("administrator", "administrator"), ("member", "member")],
                        max_length=1024,
                    ),
                ),
                (
                    "account",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="trackingserver_auth.account",
                    ),
                ),
                (
                    "user",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="trackingserver_auth.user",
                    ),
                ),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="TeamAccountMembership",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "account",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="trackingserver_auth.account",
                    ),
                ),
                (
                    "team",
                    models.OneToOneField(
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="trackingserver_auth.team",
                    ),
                ),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="APIKey",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("key_name", models.CharField(max_length=255)),
                ("key_start", models.CharField(max_length=5)),
                ("hashed_key", models.CharField(max_length=256, unique=True)),
                ("is_active", models.BooleanField(default=True)),
                (
                    "user",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="trackingserver_auth.user",
                    ),
                ),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.AddField(
            model_name="account",
            name="team",
            field=models.ManyToManyField(to="trackingserver_auth.team"),
        ),
        migrations.CreateModel(
            name="UserTeamMembership",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "team",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="trackingserver_auth.team",
                    ),
                ),
                (
                    "user",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="trackingserver_auth.user",
                    ),
                ),
            ],
            options={
                "unique_together": {("user", "team")},
            },
        ),
    ]
